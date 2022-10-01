import copy
import json
import os
import re

from datetime import datetime
from jinja2 import Environment, FileSystemLoader
from lib.baseclasses import (
    DEFAULT_SOURCE_ALIAS,
    WRITE_DISPOSITION_MAP,
    Analytic,
    AnalyticType,
    Condition,
    ConversionType,
    converttoobj,
    Field,
    JoinType,
    Join,
    LogicOperator,
    Operator,
    SourceTable,
    SQLTask,
    SQLParameter,
    Task,
    TaskOperator,
    TableType,
    todict,
    UpdateTask,
    WriteDisposition,
)
from lib.helper import FileType, format_comment, format_description
from lib.logger import format_message, ILogger
from operator import itemgetter

__all__ = [
    "create_sql_file",
    "create_sql",
]

pattern = r"^((?P<table>[a-zA-Z0-9_\{\}]+\.[a-zA-Z0-9_\{\}]+)(?:\.))?(?P<column>[a-zA-Z0-9_%'(), ]+)$"
destination_prefix = r"dim|mart|fact"
td_prefix = "td"


def create_sql_file(
    logger: ILogger,
    task: Task,
    file_path: str = "./dags/sql/",
    dataset_staging: str = None,
    job_id: str = None,
) -> str:
    """
    > This function takes a task and creates a SQL file for it

    Args:
      logger (ILogger): ILogger - this is the logger object that we created in the previous step.
      task (Task): This is the task object that we created in the previous section.
      file_path (str): The path to the directory where the SQL file will be created. Defaults to
    ./dags/sql/
      dataset_staging (str): This is the name of the staging table that will be created.

    Returns:
      The file path of the sql file that was created.
    """

    logger.info(f"STARTED".center(100, "-"))
    sql = create_sql(logger, task, dataset_staging)
    file_loader = FileSystemLoader("./templates")
    env = Environment(loader=file_loader)

    template = env.get_template("template_sql.txt")
    output = template.render(
        sql=sql,
        task_id=task.task_id,
        job_id=job_id if job_id else task.task_id,
        description=format_description(task.description, "Description", FileType.SQL),
        created_date=datetime.now().strftime("%d %b %Y"),
        author=task.author,
    )

    sql_file = os.path.join(file_path, f"{task.task_id}.sql")
    with open(sql_file, "w") as outfile:
        outfile.write(output)

    logger.info(f"COMPLETED SUCCESSFULLY".center(100, "-"))
    return sql_file


def create_sql(logger: ILogger, task: Task, dataset_staging: str = None) -> str:
    """
    It takes a task object and returns a string of SQL

    Args:
      logger (ILogger): ILogger - this is the logger object that is passed to the function.
      task (Task): the task object
      dataset_staging (str): The name of the staging dataset.

    Returns:
      A string of SQL code
    """

    logger.info(f"STARTED".center(100, "-"))

    params = SQLParameter(
        task.parameters.get("destination_table"),
        TableType[task.parameters.get("target_type")],
        task.parameters.get("driving_table"),
        converttoobj(task.parameters.get("source_to_target"), ConversionType.SOURCE),
        converttoobj(task.parameters.get("source_tables"), ConversionType.SOURCETABLES),
        WriteDisposition[
            task.parameters.get("write_disposition", "WRITETRUNCATE").upper()
        ],
        task.parameters.get("sql"),
        converttoobj(task.parameters.get("joins"), ConversionType.JOIN),
        converttoobj(task.parameters.get("where"), ConversionType.WHERE),
        converttoobj(task.parameters.get("delta"), ConversionType.DELTA),
        task.parameters.get("destination_dataset", "{{dataset_publish}}"),
        dataset_staging,
        converttoobj(task.parameters.get("history"), ConversionType.ANALYTIC),
        task.parameters.get("block_data_check"),
        task.parameters.get("build_artifacts"),
    )

    sqltask = SQLTask(
        copy.copy(task.task_id),
        TaskOperator[task.operator],
        params,
        copy.copy(task.author),
        copy.deepcopy(task.dependencies),
        copy.copy(task.description),
    )

    logger.info(f"creating sql for table type {sqltask.parameters.target_type.name}")
    if sqltask.parameters.write_disposition == WriteDisposition.DELETE:
        sql = create_truncate_table_sql(
            logger,
            sqltask,
        )
    elif sqltask.parameters.target_type == TableType.TYPE1:
        sql = create_type_1_sql(
            logger,
            sqltask,
        )
    elif sqltask.parameters.target_type == TableType.HISTORY:
        sql = create_type_2_sql(
            logger,
            sqltask,
        )

    sql.append("\n")

    outp = "\n".join(sql)
    logger.info(f"COMPLETED SUCCESSFULLY".center(100, "-"))
    return outp


def create_delta_conditions(logger: ILogger, task: SQLTask) -> list[Condition]:
    """
    > This function creates a list of conditions for the delta load

    Args:
      logger (ILogger): ILogger - the logger object
      task (SQLTask): SQLTask

    Returns:
      A list of Condition objects
    """

    logger.info(f"STARTED".center(100, "-"))

    outp = []

    if task.parameters.delta:
        delta = task.parameters.delta
        logger.debug(f"delta object: {json.dumps(todict(delta), indent=4)}")
        if delta.field.transformation:
            field = delta.field.transformation
        else:
            field = delta.field.source(task.parameters.driving_table)

        lower_bound = convert_lower_bound(logger, delta.lower_bound)

        outp.append(
            Condition(
                [field, lower_bound],
                operator=Operator.GE,
            )
        )

        upper_bound = None
        if delta.lower_bound.upper() == "@LOWER_DATE_BOUND":
            upper_bound = "parse_timestamp('%d-%b-%Y %H:%M:%E6S', @upper_date_bound)"
        elif delta.upper_bound:
            upper_bound = (
                (f"date_add({lower_bound}, interval {delta.upper_bound} second)")
                if delta.upper_bound > 0
                else "timestamp(2999-12-31 23:59:59)"
            )

        if upper_bound:
            outp.append(
                Condition(
                    [field, upper_bound],
                    operator=Operator.LT,
                )
            )

    logger.info(f"COMPLETED SUCCESSFULLY".center(100, "-"))
    return outp


def convert_lower_bound(logger: ILogger, lower_bound: str) -> str:
    """
    It converts the lower bound of the date range to a string that can be used in a SQL query

    Args:
      logger (ILogger): ILogger - this is the logger object that is passed to the function.
      lower_bound (str): The lower bound of the date range to be queried.

    Returns:
      A string that is the lower bound of the date range.
    """

    logger.info(f"STARTED".center(100, "-"))
    if lower_bound.upper() == "$TODAY":
        outp = "timestamp(current_date)"
    elif lower_bound.upper() == "$YESTERDAY":
        outp = "timestamp(date_sub(current_date, interval 1 day))"
    elif lower_bound.upper() == "$THISWEEK":
        outp = "(select timestamp(date_sub(current_date, interval (if(dayofweek > 1,-2,5) + dayofweek) day)) from (SELECT EXTRACT(DAYOFWEEK FROM current_date) dayofweek))"
    elif lower_bound.upper() == "$THISMONTH":
        outp = "timestamp(date_add(last_day(date_sub(current_date, interval 1 month)), interval 1 day))"
    elif lower_bound.upper() == "@LOWER_DATE_BOUND":
        outp = "parse_timestamp('%d-%b-%Y %H:%M:%S', @lower_date_bound)"
    else:
        outp = lower_bound

    logger.info(f"COMPLETED SUCCESSFULLY".center(100, "-"))
    return outp


def create_type_1_sql(
    logger: ILogger,
    task: SQLTask,
) -> list:
    """
    > This function creates the SQL for a Type 1 load

    Args:
      logger (ILogger): ILogger - this is the logger object that is passed to the function.
      task (SQLTask): The SQLTask object that contains all the information about the task.

    Returns:
      A list of SQL statements
    """

    logger.info(f"STARTED".center(100, "-"))

    delta = create_delta_conditions(logger, task)
    wtask = copy.deepcopy(task)
    sql = []

    if len(delta):
        if wtask.parameters.where is None:
            wtask.parameters.where = []

        for d in delta:
            wtask.parameters.where.append(d)

        wtask.parameters.write_disposition = WriteDisposition.WRITETRANSIENT
        wtask.parameters.destination_dataset = wtask.parameters.staging_dataset
        wtask.parameters.destination_table = re.sub(
            destination_prefix,
            td_prefix,
            f"{wtask.parameters.destination_table}_p1"
            if task.parameters.write_disposition != WriteDisposition.WRITETRANSIENT
            else wtask.parameters.destination_table,
            0,
            re.MULTILINE,
        )

        sql.append(
            create_sql_comment(
                logger, "Create p1 with initial select and transformation."
            )
        )
    else:
        dw_index = 1
        for i, field in enumerate(wtask.parameters.source_to_target):
            if not field.pk:
                dw_index = i + 1
                break
        if wtask.parameters.write_disposition != WriteDisposition.WRITETRANSIENT:
            wtask.parameters.source_to_target.insert(
                dw_index,
                Field(
                    transformation=f"current_timestamp()",
                    data_type="TIMESTAMP",
                    name="dw_last_modified_dt",
                ),
            )

        sql.append(create_sql_comment(logger, "Create target table."))

    sql.append(
        create_table_query(
            logger,
            wtask,
        )
    )

    if (
        len(delta)
        and task.parameters.write_disposition != WriteDisposition.WRITETRANSIENT
    ):
        wtask.parameters.driving_table = f"{wtask.parameters.destination_dataset}.{wtask.parameters.destination_table}"
        wtask.parameters.destination_dataset = task.parameters.destination_dataset
        wtask.parameters.destination_table = task.parameters.destination_table

        sql.extend(create_delta_comparisons(logger, wtask))

    logger.info(f"COMPLETED SUCCESSFULLY".center(100, "-"))
    return sql


def create_delta_comparisons(logger: ILogger, task: SQLTask) -> list[str]:
    """
    This function creates a comparison between the source and target tables for delta loads.

    task.parameters.driving_table will be joined to task.parameters.destination_table to
    identify if a record is new and needs inserted or is existing and nees an update.

    Args:
      logger (ILogger): ILogger - a logger object
      task (SQLTask): The SQLTask object that contains all the parameters for the query

    Returns:
      A tuple of strings
    """
    logger.info(f"STARTED".center(100, "-"))
    sql = [
        create_sql_comment(
            logger,
            "As table is loaded with delta load, identify which records exist already and so need updated, and which records require a fresh insert.  Create new column 'row_count' and set value 1 for insert and 2 for update.",
        )
    ]
    # first we need to identify which records exist already and need updated
    # and which records need inserted. Update the task object to source from p1
    # and join to target to compare records
    dtask = copy.deepcopy(task)

    keys = dtask.primary_keys

    dtask.parameters.joins = [
        Join(
            SourceTable(
                dataset_name=dtask.parameters.destination_dataset,
                table_name=dtask.parameters.destination_table,
                alias="trg",
            ),
            [
                Condition(
                    [
                        f"src.{k}",
                        f"trg.{k}",
                    ],
                    operator=Operator.EQ,
                )
                for k in keys
            ],
        )
    ]
    m = re.search(
        r"^(?P<dataset_name>[a-z_\-0-9]+).(?P<table_name>[a-z_\-0-9]+)",
        dtask.parameters.driving_table,
        re.IGNORECASE,
    )
    dtask.parameters.source_to_target = [
        Field(
            name=field.name,
            source_column=field.name,
            source_table=SourceTable(
                dataset_name=m.group("dataset_name"),
                table_name=m.group("table_name"),
                alias="src",
            ),
        )
        for field in dtask.parameters.source_to_target
    ]

    dtask.parameters.source_to_target.append(
        Field(
            transformation=f"if(trg.{keys[0]} is null, 1, 2)",
            name="row_action",
        )
    )

    regex = r"^.+\.(?P<table_name>[a-z_\-0-9]+)(?P<table_index>\d+)$"
    m = re.match(regex, task.parameters.driving_table, re.IGNORECASE)
    table_index = int(m.group("table_index") if m else 0)
    table_name = m.group("table_name")

    dtask.parameters.destination_table = f"{table_name}{str(table_index + 1)}"
    dtask.parameters.destination_dataset = dtask.parameters.staging_dataset
    dtask.parameters.where = []
    dtask.parameters.write_disposition = WriteDisposition.WRITETRANSIENT

    sql.append(
        create_table_query(
            logger,
            dtask,
        )
    )

    # for all new inserts (row_action = 1) create an insert into
    sql.append(
        create_sql_comment(
            logger,
            "For all new inserts (row_acount = 1), insert into target table.",
        )
    )
    iitask = copy.deepcopy(task)
    iitask.parameters.joins = []
    iitask.parameters.where = [
        Condition(
            [
                f"src.row_action",
                "1",
            ],
            operator=Operator.EQ,
        )
    ]
    driving_table = SourceTable(
        dataset_name=iitask.parameters.staging_dataset,
        table_name=dtask.parameters.destination_table,
        alias="src",
    )

    iitask.parameters.driving_table = (
        f"{driving_table.dataset_name}.{driving_table.table_name}"
    )

    iitask.parameters.source_to_target = [
        Field(
            name=field.name,
            source_column=field.name,
            source_table=driving_table,
            pk=field.pk,
        )
        for field in task.parameters.source_to_target
    ]

    dw_index = 1

    iitask.parameters.source_to_target.insert(
        dw_index,
        Field(
            transformation=f"current_timestamp()",
            data_type="TIMESTAMP",
            name="dw_last_modified_dt",
        ),
    )

    iitask.parameters.source_to_target.insert(
        dw_index,
        Field(
            transformation=f"current_timestamp()",
            data_type="TIMESTAMP",
            name="dw_created_dt",
        ),
    )

    iitask.parameters.write_disposition = WriteDisposition.WRITEAPPEND

    sql.append(
        create_table_query(
            logger,
            iitask,
        )
    )

    # for all updates (row_action = 2) create an update query
    if task.parameters.target_type in [TableType.TYPE1]:
        sql.append(
            create_sql_comment(
                logger,
                "For all updates (row_action = 2), make update to target table to set new values.",
            )
        )
        source_to_target = [
            Field(
                field.name,
                source_column=field.name,
                source_table=driving_table,
            )
            for field in task.parameters.source_to_target
            if not field.pk
        ]
    elif task.parameters.target_type in [TableType.HISTORY]:
        sql.append(
            create_sql_comment(
                logger, "For all updates (row_action = 2), set effective_to_dt."
            )
        )
        source_to_target = [
            Field(
                "effective_to_dt",
                source_column="effective_to_dt",
                source_table=driving_table,
            )
        ]

    source_to_target.append(
        Field(
            "dw_last_modified_dt",
            data_type="TIMESTAMP",
            transformation="current_timestamp()",
        )
    )

    update_conditions = [
        Condition(
            [
                f"src.{field.name}",
                f"trg.{field.name}",
            ],
            operator=Operator.EQ,
        )
        for field in iitask.parameters.source_to_target
        if field.pk
    ]

    update_conditions.append(
        Condition(
            [
                f"src.row_action",
                "2",
            ],
            operator=Operator.EQ,
        )
    )

    utask = UpdateTask(
        iitask.parameters.destination_dataset,
        task.parameters.destination_table,
        iitask.parameters.staging_dataset,
        dtask.parameters.destination_table,
        source_to_target,
        {
            iitask.parameters.driving_table: "src",
            f"{iitask.parameters.destination_dataset}.{iitask.parameters.destination_table}": "trg",
        },
        update_conditions,
    )

    sql.append(
        create_update_query(
            logger,
            utask,
        )
    )

    logger.info(f"COMPLETED SUCCESSFULLY".center(100, "-"))
    return sql


def create_type_2_sql(
    logger: ILogger,
    task: SQLTask,
) -> list:
    """
    This function creates a series of SQL statements that will create a table that contains the current
    and previous values of the driving columns.

    The function is broken down into three parts:

    1. Create a table that contains the current and previous values of the driving columns.
    2. Create a table that contains the current and previous values of the driving columns and the delta
    between the two.
    3. Create a table that contains the current and previous values of the driving columns and the delta
    between the two and the effective_to_dt.

    The first part of the function creates a table that contains the current and previous values of the
    driving columns.

    The second part of the function creates a table that contains the current and previous values of the
    driving columns and the delta between the two.

    The third part of the function creates a table that contains the current and previous values of the
    driving columns and the delta between the two

    Args:
      logger (ILogger): ILogger,
      task (SQLTask): SQLTask

    Returns:
      A list of SQL statements
    """
    logger.info(f"STARTED".center(100, "-"))
    sql = [
        create_sql_comment(
            logger,
            "Create p1 table, pulling all source data and use LAG to create columns containing previous values for driving columns.",
        ),
    ]
    td_table = re.sub(r"^[a-zA-Z]+_", "td_", task.parameters.destination_table)
    logger.info(
        format_message(
            f'create sql for transient table, pull source data and previous columns - "{task.parameters.staging_dataset}.{td_table}_p1"'
        )
    )

    # first we create p1, this table contains required columns plus previous
    # value for driving tables.  Previous values are used later to complete CDC
    analytics = create_type_2_analytic_list(logger, task)
    wtask = copy.deepcopy(task)

    wtask.parameters.destination_table = f"{td_table}_p1"
    wtask.parameters.destination_dataset = wtask.parameters.staging_dataset
    wtask.parameters.write_disposition = WriteDisposition.WRITETRANSIENT

    for analytic in analytics:
        wtask.add_analytic(analytic)
    delta = create_delta_conditions(logger, task)
    if len(delta):
        if wtask.parameters.where is None:
            wtask.parameters.where = []

        for d in delta:
            wtask.parameters.where.append(d)

    sql.append(
        create_table_query(
            logger,
            wtask,
        )
    )

    # if delta, take all primary keys identified and add full history to p1
    if len(delta):
        logger.info(
            format_message(
                f'create sql to insert delta history into transient table - "{task.parameters.staging_dataset}.{td_table}_p1"'
            )
        )

        sql.append(
            create_sql_comment(
                logger,
                "For all objects being tracked extract any existing history from source and insert into p1 table.  This allows us to re-calculate entire history and to exclude any records already covered via CDC.",
            )
        )

        join_on = [
            Condition(
                fields=[
                    f"p1.{field.name}",
                    f"{field.source(task.parameters.driving_table)}",
                ],
                operator=Operator.EQ,
            )
            for field in task.parameters.source_to_target
            if field.hk
        ]

        delta_join = Join(
            join_type=JoinType.INNER,
            right=SourceTable(
                dataset_name=wtask.parameters.staging_dataset,
                table_name=f"{td_table}_p1",
                alias="p1",
            ),
            on=join_on,
        )

        delta_task = SQLTask(
            "delta_task",
            TaskOperator.CREATETABLE,
            copy.deepcopy(task.parameters),
            copy.copy(task.author),
        )
        delta_task.parameters.write_disposition = WriteDisposition.WRITEAPPEND
        delta_task.parameters.destination_table = f"{td_table}_p1"
        delta_task.parameters.destination_dataset = (
            delta_task.parameters.staging_dataset
        )

        if delta_task.parameters.joins:
            delta_task.parameters.joins.append(delta_join)
        else:
            delta_task.parameters.joins = [delta_join]

        delta_where = create_type_2_delta_condition(logger, task)

        if delta_task.parameters.where:
            delta_task.parameters.where.append(delta_where)
        else:
            delta_task.parameters.where = [delta_where]

        for analytic in analytics:
            delta_task.add_analytic(analytic)

        sql.append(
            create_table_query(
                logger,
                delta_task,
            )
        )

    logger.info(
        format_message(
            f'create sql for transient table, complete CDC - "{task.parameters.staging_dataset}.{td_table}_p2"'
        )
    )

    sql.append(
        create_sql_comment(
            logger,
            "Complete Change Data Capture (CDC) to remove any source records that don't represent a change in data - i.e. if the current value is equal to the previous then exclude.",
        )
    )

    # second we complete CDC.  We create a new task object using our p1 table as
    # driving table
    p2_task = SQLTask(
        "p2_task",
        TaskOperator.CREATETABLE,
        copy.deepcopy(task.parameters),
        copy.copy(task.author),
    )
    p2_task.parameters.write_disposition = WriteDisposition.WRITETRANSIENT
    p2_task.parameters.destination_table = f"{td_table}_p2"
    p2_task.parameters.destination_dataset = p2_task.parameters.staging_dataset
    p2_task.parameters.driving_table = (
        f"{task.parameters.staging_dataset}.{td_table}_p1"
    )
    p1_source_table = SourceTable(
        dataset_name=task.parameters.staging_dataset,
        table_name=f"{td_table}_p1",
        alias="src",
    )
    p2_task.parameters.source_to_target = [
        Field(
            name=field.name,
            source_table=p1_source_table,
        )
        for field in task.parameters.source_to_target
    ]
    p2_task.parameters.where = [
        Condition(
            [
                f"ifnull(cast(src.{c.name} as string),'NULL')",
                f"ifnull(cast(src.prev_{c.name} as string),'NULL')",
            ],
            operator=Operator.NE,
            condition=LogicOperator.OR,
        )
        for c in task.parameters.history.driving_column
    ]
    p2_task.parameters.joins = None

    sql.append(
        create_table_query(
            logger,
            p2_task,
        )
    )

    logger.info(
        format_message(
            f'create sql for transient table, add/replace effective_to_dt with lead - "{task.parameters.staging_dataset}.{td_table}"'
        )
    )

    # next step takes the data following the CDC and adds the effective_to_dt
    sql.append(
        create_sql_comment(
            logger,
            "Complete a LEAD analytic on effective_from_dt to identify the closing datetime value and set as effective_to_dt.  Where no closing date, default to HIGH DATE.",
        )
    )

    p3_task = SQLTask(
        "p3_task",
        TaskOperator.CREATETABLE,
        copy.deepcopy(task.parameters),
        copy.copy(task.author),
    )
    p3_task.parameters.driving_table = (
        f"{task.parameters.staging_dataset}.{td_table}_p2"
    )
    p3_task.parameters.destination_table = f"{td_table}_p3"
    p3_task.parameters.destination_dataset = task.parameters.staging_dataset
    p2_source_table = SourceTable(
        dataset_name=task.parameters.staging_dataset,
        table_name=f"{td_table}_p2",
        alias="src",
    )
    p3_task.parameters.source_to_target = [
        Field(name=c.name, source_column=c.name, source_table=p2_source_table, pk=c.pk)
        for c in task.parameters.source_to_target
    ]

    p3_task.parameters.joins = None
    p3_task.parameters.where = None
    p3_task.parameters.write_disposition = WriteDisposition.WRITETRANSIENT

    to_index = None
    for i, col in enumerate(task.parameters.source_to_target):
        if col.name == "effective_from_dt_seq":
            to_index = i + 1
            break

    p3_task.add_analytic(
        Analytic(
            [
                Field(
                    name=p.name,
                    source_column=p.name,
                    source_table=p2_source_table,
                )
                for p in task.parameters.history.partition
            ],
            [
                Field(
                    name="effective_from_dt",
                    source_column="effective_from_dt",
                    source_table=p2_source_table,
                ),
                Field(
                    name="effective_from_dt_csn_seq",
                    source_column="effective_from_dt_csn_seq",
                    source_table=p2_source_table,
                ),
                Field(
                    name="effective_from_dt_seq",
                    source_column="effective_from_dt_seq",
                    source_table=p2_source_table,
                ),
            ],
            offset=1,
            default="timestamp('2999-12-31 23:59:59')",
            type=AnalyticType.LEAD,
            column=Field(name="effective_to_dt", source_column="effective_from_dt"),
        ),
        to_index,
    )

    sql.append(
        create_table_query(
            logger,
            p3_task,
        )
    )

    td_task = SQLTask(
        "td_task",
        TaskOperator.CREATETABLE,
        copy.deepcopy(p3_task.parameters),
        copy.copy(p3_task.author),
    )

    td_task.parameters.driving_table = (
        f"{task.parameters.staging_dataset}.{td_table}_p3"
    )
    td_task.parameters.destination_dataset = task.parameters.destination_dataset
    td_task.parameters.destination_table = task.parameters.destination_table

    p3_source_table = SourceTable(
        dataset_name=task.parameters.staging_dataset,
        table_name=f"{td_table}_p3",
        alias="src",
    )
    td_task.parameters.source_to_target = [
        Field(name=c.name, source_column=c.name, source_table=p3_source_table, pk=c.pk)
        for c in p3_task.parameters.source_to_target
    ]

    if len(delta):
        sql.extend(create_delta_comparisons(logger, td_task))

    else:
        sql.append(
            create_sql_comment(
                logger,
                "Complete final insert into target.",
            )
        )
        td_task.parameters.source_to_target.insert(
            1,
            Field(
                transformation=f"current_timestamp()",
                data_type="TIMESTAMP",
                name="dw_last_modified_dt",
            ),
        )
        sql.append(
            create_table_query(
                logger,
                td_task,
            )
        )

    logger.info(f"COMPLETED SUCCESSFULLY".center(100, "-"))
    return sql


def create_type_2_analytic_list(logger: ILogger, task: SQLTask) -> list:
    """
    > It creates a list of `Analytic` objects for the `LAG` analytic function

    Args:
      logger (ILogger): ILogger - a logger object that can be used to log messages
      task (SQLTask): SQLTask object

    Returns:
      A list of Analytic objects
    """
    logger.info(f"STARTED".center(100, "-"))
    outp = []
    history = task.parameters.history if task.parameters.history else []
    for c in history.driving_column:
        analytic = Analytic(
            partition=history.partition,
            order=history.order,
            type=AnalyticType.LAG,
            column=Field(
                name=f"prev_{c.name}",
                source_column=c.source_column,
                source_table=c.source_table,
            ),
            offset=1,
            default=None,
        )
        outp.append(analytic)

    logger.info(f"COMPLETED SUCCESSFULLY".center(100, "-"))
    return outp


def create_type_2_delta_condition(logger: ILogger, task: SQLTask) -> dict:
    """
    > This function creates a dictionary that represents a condition that can be used to filter data
    based on a date field

    Args:
      logger (ILogger): ILogger - this is the logger object that is passed to the function.
      task (SQLTask): SQLTask

    Returns:
      A dictionary with the operator and fields.
    """

    logger.info(f"STARTED".center(100, "-"))

    outp = None

    if task.parameters.delta:
        delta = task.parameters.delta
        logger.debug(f"delta object: {delta}")
        if delta.field.transformation:
            field = delta.field.transformation
        else:
            field = delta.field.source(task.parameters.driving_table)

        if delta.lower_bound == "$TODAY":
            upper_bound = "timestamp(current_date)"
        elif delta.lower_bound == "$YESTERDAY":
            upper_bound = "timestamp(date_sub(current_date, interval 1 day))"
        elif delta.lower_bound == "$THISWEEK":
            upper_bound = "(select timestamp(date_sub(current_date, interval (if(dayofweek > 1,-2,5) + dayofweek) day)) from (SELECT EXTRACT(DAYOFWEEK FROM current_date) dayofweek))"
        elif delta.lower_bound == "$THISMONTH":
            upper_bound = "timestamp(date_add(last_day(date_sub(current_date, interval 1 month)), interval 1 day))"
        else:
            upper_bound = delta.lower_bound

        outp = Condition([field, upper_bound], operator=Operator.LT)

    logger.info(f"COMPLETED SUCCESSFULLY".center(100, "-"))
    return outp


def create_truncate_table_sql(
    logger: ILogger,
    task: SQLTask,
) -> str:
    """
    > This function creates a SQL statement to truncate a table

    Args:
      logger (ILogger): ILogger,
      task (SQLTask): SQLTask

    Returns:
      A string
    """

    logger.info(f"STARTED".center(100, "-"))
    sql = [
        f"{task.parameters.destination_dataset}.{task.parameters.destination_table}:DELETE:",
        f"truncate table {task.parameters.destination_dataset}.{task.parameters.destination_table};",
    ]
    logger.info(f"COMPLETED SUCCESSFULLY".center(100, "-"))
    return sql


def create_table_query(
    logger: ILogger,
    task: SQLTask,
) -> str:
    """
    It takes a SQLTask object and returns a string of SQL

    Args:
      logger (ILogger): ILogger - this is the logger object that is passed to the function.
      task (SQLTask): SQLTask object

    Returns:
      A string of SQL code.
    """
    logger.info(f"STARTED".center(100, "-"))
    sql = []
    # create working task object so it can be edited without impacting
    # original
    wtask = copy.deepcopy(task)

    # write truncate disposition from config is translated to a truncate statement followed
    # by an append.
    if wtask.parameters.write_disposition == WriteDisposition.WRITETRUNCATE:
        sql.extend(
            [
                f"{wtask.parameters.destination_dataset}.{wtask.parameters.destination_table}:DELETE:",
                f"truncate table {wtask.parameters.destination_dataset}.{wtask.parameters.destination_table};",
            ]
        )

    sql.append(
        f"{wtask.parameters.destination_dataset}.{wtask.parameters.destination_table}:{WRITE_DISPOSITION_MAP.get(wtask.parameters.write_disposition.value)}:"
    )

    frm, where = itemgetter("from", "where")(create_sql_conditions(logger, wtask))

    select = create_sql_select(logger, wtask)
    query_list = [
        ",\n".join(select),
        "\n",
        "\n".join(frm),
        "\n",
        "\n".join(where),
        ";\n",
    ]

    sql.append("".join(query_list))
    outp = "\n".join(sql)
    logger.info(f"COMPLETED SUCCESSFULLY".center(100, "-"))
    return outp


def create_update_query(logger: ILogger, task: UpdateTask) -> str:
    """
    > It creates an update query for a given update task

    Args:
      logger (ILogger): ILogger - a logger object that will be used to log messages
      task (UpdateTask): UpdateTask object

    Returns:
      A string that is a SQL query.
    """
    logger.info(f"STARTED".center(100, "-"))

    outp = [
        f"{task.target_dataset}.{task.target_table}:UPDATE:",
        f"update {task.target_dataset}.{task.target_table} trg",
    ]

    st = []
    pad = max([len(f.name) for f in task.source_to_target])

    for i, f in enumerate(task.source_to_target):
        prefix = "   set " if i == 0 else "       "
        source = f.source(f"{task.source_dataset}.{task.source_table}")

        if f.transformation:
            source = f.transformation
        target = f"trg.{f.name.ljust(pad)}"

        st.append(f"{prefix}{target} = {source}")

    outp.append(",\n".join(st))
    source_table = f"{task.source_dataset}.{task.source_table}"
    outp.append(f"  from {source_table} {task.tables[source_table]}")

    outp.append("\n".join(create_sql_where(logger, task.where)))
    outp.append(";\n")
    logger.info(f"COMPLETED SUCCESSFULLY".center(100, "-"))
    return "\n".join(outp)


def create_sql_select(logger: ILogger, task: SQLTask) -> str:
    """
    > This function creates a SQL select statement from a SQLTask object

    Args:
      logger (ILogger): ILogger - this is the logger that is passed in from the calling function.
      task (SQLTask): SQLTask

    Returns:
      A string
    """

    logger.info(f"STARTED".center(100, "-"))
    logger.debug(
        f"""creating select list from
                               task - {task}"""
    )
    select = []
    # for each column in the source_to_target we identify the source table and column,
    # or where there is transformation use that in place of the source table and column,
    # and target column.
    for i, column in enumerate(task.parameters.source_to_target):
        prefix = "select " if i == 0 else "       "

        source = column.source(DEFAULT_SOURCE_ALIAS)

        alias = (
            column.name.rjust(
                max(
                    (60 - len(f"{prefix}{source}") + len(column.name)) - 1,
                    1 + len(column.name),
                )
            )
            if column.name != column.source_column or column.transformation
            else ""
        )

        select.append(f"{prefix}{source}{alias}")

    logger.info(f"COMPLETED SUCCESSFULLY".center(100, "-"))
    return select


def create_sql_conditions(logger: ILogger, task: SQLTask) -> dict:
    """
    > This function creates the `from` and `where` clauses of a SQL statement

    Args:
      logger (ILogger): ILogger - the logger object
      task (SQLTask): SQLTask

    Returns:
      A dictionary with two keys: from and where.
    """
    logger.info(f"STARTED".center(100, "-"))

    frm = [f"  from {task.parameters.driving_table} {DEFAULT_SOURCE_ALIAS}"]
    logger.info(f"identifying join conditions")
    if task.parameters.joins:
        for join in task.parameters.joins:

            frm.append(
                f"{join.join_type.value.rjust(6)} join {join.right.dataset_name}.{join.right.table_name} {join.right.alias}"
            )

            pad = max([len(c.fields[0]) for c in join.on])
            for j, condition in enumerate(join.on):
                on_prefix = "(    " if len(join.on) > 1 and j == 0 else ""
                on_suffix = ")" if len(join.on) > 1 and len(join.on) == j + 1 else ""
                if j == 0:
                    prefix = "    on "
                elif condition.condition.value:
                    prefix = f"{condition.condition.value.rjust(11)} "
                else:
                    prefix = "        and "

                left = f"{condition.fields[0]}"
                right = f"{condition.fields[1]}"

                open_bracket = (
                    "(" if condition.operator.value in ["in", "not in"] else ""
                )
                close_bracket = (
                    ")" if condition.operator.value in ["in", "not in"] else ""
                )

                frm.append(
                    f"{prefix}{on_prefix}{left.ljust(pad)} {condition.operator.value} {open_bracket}{right}{close_bracket}{on_suffix}"
                )

    where = (
        create_sql_where(
            logger,
            task.parameters.where,
        )
        if task.parameters.where
        else ""
    )

    outp = {"from": frm, "where": where}

    logger.info(f"COMPLETED SUCCESSFULLY".center(100, "-"))
    return outp


def create_sql_where(logger: ILogger, conditions: list[Condition]) -> str:
    """
    It takes a list of conditions and returns a list of strings that can be used in a SQL where clause

    Args:
      logger (ILogger): ILogger - this is the logger object that is passed to the function.
      conditions (list[Condition]): list[Condition]

    Returns:
      A list of strings
    """
    logger.info(f"STARTED".center(100, "-"))
    logger.debug(
        format_message(
            f"""creating where conditions:
                               conditions  - {conditions}"""
        )
    )

    where = []

    pad = max([len(c.fields[0]) for c in conditions]) if len(conditions) > 0 else 0
    for i, condition in enumerate(conditions):
        if i == 0:
            prefix = " where "
        elif condition.condition.value:
            prefix = f"{condition.condition.value.rjust(6)} "
        else:
            prefix = "   and "

        left_table = condition.fields[0] if condition.fields[0] else ""
        right_table = condition.fields[1] if condition.fields[1] else ""

        open_bracket = "(" if condition.operator.value in ["in", "not in"] else ""
        close_bracket = ")" if condition.operator.value in ["in", "not in"] else ""
        where.append(
            f"{prefix}{left_table.ljust(pad)} {condition.operator.value} {open_bracket}{right_table}{close_bracket}"
        )

    logger.info(f"COMPLETED SUCCESSFULLY".center(100, "-"))
    return where


def create_sql_comment(logger: ILogger, comment: str) -> str:
    logger.info(f"STARTED".center(100, "-"))

    sql_comment = f"{''.ljust(81,'-')}\n{format_comment(comment, FileType.SQL)}\n{''.ljust(81,'-')}"

    logger.info(f"COMPLETED SUCCESSFULLY".center(100, "-"))
    return sql_comment
