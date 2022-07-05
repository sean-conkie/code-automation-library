import copy
import re

from datetime import datetime
from enum import Enum
from jinja2 import Environment, FileSystemLoader
from lib.baseclasses import (
    Analytic,
    AnalyticType,
    Condition,
    ConversionType,
    Field,
    Join,
    LogicOperator,
    Operator,
    JoinType,
    Task,
    TaskOperator,
    WriteDisposition,
    UpdateTask,
    SQLTask,
    SQLParameter,
    TableType,
    converttoobj,
)
from lib.logger import ILogger, pop_stack
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

    logger.info(f"{pop_stack()} - STARTED".center(100, "-"))
    sql = create_sql(logger, task, dataset_staging)
    file_loader = FileSystemLoader("./templates")
    env = Environment(loader=file_loader)

    template = env.get_template("template_sql.txt")
    output = template.render(
        sql=sql,
        task_id=task.task_id,
        created_date=datetime.now().strftime("%d %b %Y"),
    )

    sql_file = f"{file_path}{task.task_id}.sql"
    with open(sql_file, "w") as outfile:
        outfile.write(output)

    logger.info(f"{pop_stack()} - COMPLETED SUCCESSFULLY".center(100, "-"))
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

    logger.info(f"{pop_stack()} - STARTED".center(100, "-"))

    params = SQLParameter(
        task.parameters.get("destination_table"),
        TableType[task.parameters.get("target_type")],
        task.parameters.get("driving_table"),
        converttoobj(task.parameters.get("source_to_target"), ConversionType.SOURCE),
        WriteDisposition[
            task.parameters.get("write_disposition", "WRITE_TRUNCATE").upper()
        ],
        task.parameters.get("sql"),
        converttoobj(task.parameters.get("joins"), ConversionType.JOIN),
        converttoobj(task.parameters.get("where"), ConversionType.WHERE),
        converttoobj(task.parameters.get("delta"), ConversionType.DELTA),
        task.parameters.get("destination_dataset", "{{dataset_publish}}"),
        dataset_staging,
        converttoobj(task.parameters.get("history"), ConversionType.ANALYTIC),
        task.parameters.get("block_data_check"),
    )

    sqltask = SQLTask(
        copy.copy(task.task_id),
        TaskOperator[task.operator],
        params,
        copy.deepcopy(task.dependencies),
    )

    logger.info(
        f"{pop_stack()} - creating sql for table type {sqltask.parameters.target_type.name}"
    )
    if sqltask.parameters.target_type == TableType.TYPE1:
        sql = create_type_1_sql(
            logger,
            sqltask,
        )
    elif sqltask.parameters.target_type == TableType.HISTORY:
        sql = create_type_2_sql(
            logger,
            sqltask,
        )

    outp = "\n".join(sql)
    logger.info(f"{pop_stack()} - COMPLETED SUCCESSFULLY".center(100, "-"))
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

    logger.info(f"{pop_stack()} - STARTED".center(100, "-"))

    outp = []

    if task.parameters.delta:
        delta = task.parameters.delta
        logger.debug(f"delta object: {delta}")
        if delta.field.transformation:
            field = delta.field.transformation
        else:
            source_name = (
                delta.field.source_name
                if delta.field.source_name
                else task.parameters.driving_table
            )
            field = f"{source_name}.{delta.field.source_column}"

        lower_bound = convert_lower_bound(logger, delta.lower_bound)

        outp.append(
            Condition(
                [field, lower_bound],
                operator=Operator.GE,
            )
        )

        if delta.upper_bound:
            upper_bound = (
                (f"date_add({lower_bound}, interval {delta.upper_bound} second)")
                if delta.upper_bound > 0
                else "timestamp(2999-12-31 23:59:59)"
            )
            outp.append(
                Condition(
                    [field, upper_bound],
                    operator=Operator.LT,
                )
            )

    logger.info(f"{pop_stack()} - COMPLETED SUCCESSFULLY".center(100, "-"))
    return outp


def create_type_2_delta_condition(logger: ILogger, task: SQLTask) -> Condition:
    """
    > This function creates a condition object that can be used to filter the data in the driving table

    Args:
      logger (ILogger): ILogger - a logger object
      task (SQLTask): SQLTask

    Returns:
      A Condition object
    """

    logger.info(f"{pop_stack()} - STARTED".center(100, "-"))

    outp = None

    if task.parameters.delta:
        delta = task.parameters.delta
        logger.debug(f"delta object: {delta}")
        if delta.field.transformation:
            field = delta.field.transformation
        else:
            source_name = (
                task.parameters.driving_table
                if not delta.field.source_name
                else delta.field.source_name
            )
            field = f"{source_name}.{delta.field.source_column}"

        upper_bound = convert_lower_bound(logger, delta.lower_bound)

        outp = Condition(
            [field, upper_bound],
            operator=Operator.LT,
        )

    logger.info(f"{pop_stack()} - COMPLETED SUCCESSFULLY".center(100, "-"))
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

    logger.info(f"{pop_stack()} - STARTED".center(100, "-"))
    if lower_bound == "$TODAY":
        outp = "timestamp(current_date)"
    elif lower_bound == "$YESTERDAY":
        outp = "timestamp(date_sub(current_date, interval 1 day))"
    elif lower_bound == "$THISWEEK":
        outp = "(select timestamp(date_sub(current_date, interval (if(dayofweek > 1,-2,5) + dayofweek) day)) from (SELECT EXTRACT(DAYOFWEEK FROM current_date) dayofweek))"
    elif lower_bound == "$THISMONTH":
        outp = "timestamp(date_add(last_day(date_sub(current_date, interval 1 month)), interval 1 day))"
    else:
        outp = lower_bound

    logger.info(f"{pop_stack()} - COMPLETED SUCCESSFULLY".center(100, "-"))
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

    logger.info(f"{pop_stack()} - STARTED".center(100, "-"))

    delta = create_delta_conditions(logger, task)
    wtask = copy.deepcopy(task)

    if delta:
        for d in delta:
            wtask.parameters.where.append(d)

        wtask.parameters.write_disposition = WriteDisposition.WRITETRANSIENT
        wtask.parameters.destination_dataset = wtask.parameters.staging_dataset
        wtask.parameters.destination_table = re.sub(
            destination_prefix,
            td_prefix,
            f"{wtask.parameters.destination_table}_p1",
            0,
            re.MULTILINE,
        )
    else:
        task.parameters.source_to_target.append(
            Field(
                transformation=f"current_timestamp()",
                name="dw_last_modified_dt",
            )
        )

    sql = [
        create_table_query(
            logger,
            wtask,
        )
    ]

    if delta:
        wtask.parameters.driving_table = f"{wtask.parameters.destination_dataset}.{wtask.parameters.destination_table}"
        wtask.parameters.destination_dataset = task.parameters.destination_dataset
        wtask.parameters.destination_table = task.parameters.destination_table

        sql.extend(create_delta_comparisons(logger, wtask))

    logger.info(f"{pop_stack()} - COMPLETED SUCCESSFULLY".center(100, "-"))
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
    logger.info(f"{pop_stack()} - STARTED".center(100, "-"))
    sql = []
    # first we need to identify which records exist already and need updated
    # and which records need inserted. Update the task object to source from p1
    # and join to target to compare records
    dtask = copy.deepcopy(task)

    keys = dtask.primary_keys

    dtask.parameters.joins = [
        Join(
            f"{dtask.parameters.destination_dataset}.{dtask.parameters.destination_table}",
            [
                Condition(
                    [
                        f"{dtask.parameters.driving_table}.{k}",
                        f"{dtask.parameters.destination_dataset}.{dtask.parameters.destination_table}.{k}",
                    ],
                    operator=Operator.EQ,
                )
                for k in keys
            ],
        )
    ]

    dtask.parameters.source_to_target = [
        Field(
            name=field.name,
            source_column=field.name,
            source_name=f"{dtask.parameters.driving_table}",
        )
        for field in dtask.parameters.source_to_target
    ]

    dtask.parameters.source_to_target.append(
        Field(
            transformation=f"if({dtask.parameters.destination_dataset}.{dtask.parameters.destination_table}.{keys[0]} is null, 1, 2)",
            name="row_action",
        )
    )

    regex = r"^.+\.(?P<table_name>\w+)(?P<table_index>\d+)$"
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
    iitask = copy.deepcopy(task)
    iitask.parameters.joins = []
    iitask.parameters.where = [
        Condition(
            [
                f"{iitask.parameters.staging_dataset}.{dtask.parameters.destination_table}.row_action",
                "1",
            ],
            operator=Operator.EQ,
        )
    ]
    iitask.parameters.driving_table = (
        f"{iitask.parameters.staging_dataset}.{dtask.parameters.destination_table}"
    )

    iitask.parameters.source_to_target = [
        Field(
            name=field.name,
            source_column=field.name,
            source_name=f"{iitask.parameters.driving_table}",
        )
        for field in task.parameters.source_to_target
    ]
    iitask.parameters.source_to_target.append(
        Field(
            transformation=f"current_timestamp()",
            name="dw_created_dt",
        )
    )
    iitask.parameters.source_to_target.append(
        Field(
            transformation=f"current_timestamp()",
            name="dw_last_modified_dt",
        )
    )

    for f in iitask.parameters.source_to_target:
        if f.name in keys:
            f.pk = True

    iitask.parameters.write_disposition = WriteDisposition.WRITEAPPEND

    sql.append(
        create_table_query(
            logger,
            iitask,
        )
    )

    # for all updates (row_action = 2) create an update query
    if task.parameters.target_type in [TableType.TYPE1]:
        source_to_target = [
            Field(
                field.name,
                source_column=field.name,
                source_name=iitask.parameters.driving_table,
            )
            for field in task.parameters.source_to_target
            if not field.pk
        ]
    elif task.parameters.target_type in [TableType.HISTORY]:
        source_to_target = [
            Field(
                "effective_to_dt",
                source_column="effective_to_dt",
                source_name=f"{iitask.parameters.driving_table}",
            )
        ]

    source_to_target.append(
        Field("dw_last_modified_dt", transformation="current_timestamp()")
    )

    update_conditions = [
        Condition(
            [
                f"{iitask.parameters.destination_dataset}.{iitask.parameters.destination_table}.{field.name}",
                f"{dtask.parameters.staging_dataset}.{dtask.parameters.destination_table}.{field.name}",
            ],
            operator=Operator.EQ,
        )
        for field in iitask.parameters.source_to_target
        if field.pk
    ]

    update_conditions.append(
        Condition(
            [
                f"{iitask.parameters.driving_table}.row_action",
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
            f"{iitask.parameters.driving_table}": "src",
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

    logger.info(f"{pop_stack()} - COMPLETED SUCCESSFULLY".center(100, "-"))
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
    logger.info(f"{pop_stack()} - STARTED".center(100, "-"))
    sql = []
    td_table = re.sub(r"^[a-zA-Z]+_", "td_", task.parameters.destination_table)
    logger.info(
        f'{pop_stack()} - create sql for transient table, pull source data and previous columns - "{task.parameters.staging_dataset}.{td_table}_p1"'
    )

    # first we create p1, this table contains required columns plus previous
    # value for driving tables.  Previous values are used later to complete CDC
    analytics = create_type_2_analytic_list(logger, task)
    wtask = copy.deepcopy(task)

    for analytic in analytics:
        wtask.add_analytic(analytic)
    delta = create_delta_conditions(logger, task)
    if delta:
        for d in delta:
            wtask.parameters.where.append(d)

    sql.append(
        create_table_query(
            logger,
            wtask,
        )
    )

    # if delta, take all primary keys identified and add full history to p1
    if delta:
        join_on = []
        for field in task.parameters.source_to_target:
            if field.hk:
                join_on.append(
                    Condition(
                        fields=[
                            f"{wtask.parameters.staging_dataset}.{td_table}_p1.{field.name}",
                            f"{field.source(task.parameters.driving_table)}",
                        ],
                        operator=Operator.EQ,
                    )
                )

        delta_join = Join(
            join_type=JoinType.INNER,
            right=f"{wtask.parameters.staging_dataset}.{td_table}_p1",
            on=join_on,
        )

        delta_task = SQLTask(
            "delta_task", TaskOperator.CREATETABLE, copy.deepcopy(task.parameters)
        )
        delta_task.parameters.write_disposition = WriteDisposition.WRITEAPPEND

        if delta_task.parameters.joins:
            delta_task.parameters.joins.append(delta_join)
        else:
            delta_task.parameters.joins = [delta_join]

        delta_where = create_type_2_delta_condition(logger, task)

        if delta_task.parameters.where:
            delta_task.parameters.where.append(delta_where)
        else:
            delta_task.parameters.where = [delta_where]

        sql.append(
            create_table_query(
                logger,
                delta_task,
            )
        )

    logger.info(
        f'{pop_stack()} - create sql for transient table, complete CDC - "{task.parameters.staging_dataset}.{td_table}_p2"'
    )

    # second we complete CDC.  We create a new task object using our p1 table as
    # driving table
    p2_task = SQLTask(
        "p2_task", TaskOperator.CREATETABLE, copy.deepcopy(task.parameters)
    )
    p2_task.parameters.write_disposition = WriteDisposition.WRITETRANSIENT
    p2_task.parameters.driving_table = (
        f"{task.parameters.staging_dataset}.{td_table}_p1"
    )
    p2_task.parameters.source_to_target = [
        Field(
            transformation=f"* except({','.join([t.column.name for t in analytics])})"
        )
    ]
    p2_task.parameters.where = [
        Condition(
            [
                f"ifnull(cast({c.name} as string),'NULL')",
                f"ifnull(cast(prev_{c.name} as string),'NULL')",
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
        f'{pop_stack()} - create sql for transient table, add/replace effective_to_dt with lead - "{task.parameters.staging_dataset}.{td_table}"'
    )

    # final step takes the data following the CDC and adds the effective_to_dt
    td_task = SQLTask(
        "td_task", TaskOperator.CREATETABLE, copy.deepcopy(task.parameters)
    )
    td_task.parameters.driving_table = (
        f"{task.parameters.staging_dataset}.{td_table}_p2"
    )
    td_task.parameters.source_to_target = [
        Field(name=c.name, source_column=c.name, pk=c.pk)
        for c in task.parameters.source_to_target
    ]
    td_task.add_analytic(
        Analytic(
            [
                Field(name=p.name, source_column=p.name)
                for p in task.parameters.history.partition
            ],
            [
                Field(name="effective_from_dt", source_column="effective_from_dt"),
                Field(
                    name="effective_from_dt_csn_seq",
                    source_column="effective_from_dt_csn_seq",
                ),
                Field(
                    name="effective_from_dt_seq", source_column="effective_from_dt_seq"
                ),
            ],
            offset=1,
            default="timestamp(2999-12-31 23:59:59)",
            type=AnalyticType.LEAD,
            column=Field(name="effective_to_dt", source_column="effective_from_dt"),
        ),
    )

    if delta:
        sql.extend(create_delta_comparisons(logger, td_task))

    else:
        sql.append(
            create_table_query(
                logger,
                td_task,
            )
        )

    logger.info(f"{pop_stack()} - COMPLETED SUCCESSFULLY".center(100, "-"))
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
    logger.info(f"{pop_stack()} - STARTED".center(100, "-"))
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
            ),
            offset=1,
            default=None,
        )
        outp.append(analytic)

    logger.info(f"{pop_stack()} - COMPLETED SUCCESSFULLY".center(100, "-"))
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

    logger.info(f"{pop_stack()} - STARTED".center(100, "-"))

    outp = None

    if task.parameters.delta:
        delta = task.parameters.delta
        logger.debug(f"delta object: {delta}")
        if delta.field.transformation:
            field = delta.field.transformation
        else:
            source_name = (
                delta.field.source_name
                if delta.field.source_name
                else task.parameters.driving_table
            )
            field = f"{source_name}.{delta.field.source_column}"

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

    logger.info(f"{pop_stack()} - COMPLETED SUCCESSFULLY".center(100, "-"))
    return outp


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
    logger.info(f"{pop_stack()} - STARTED".center(100, "-"))
    sql = []
    # create working task object so it can be edited without impacting
    # original
    wtask = copy.deepcopy(task)

    join_char = ",\n"
    # we need to create a list of insert columns for insert into statements
    # as we can't run a check on the ordering of the columns in BigQuery to compare
    # to the config.
    insert_columns = [
        f"{'       ' if i > 0 else ''}{c.name}"
        for i, c in enumerate(wtask.parameters.source_to_target)
    ]
    table_operation_suffix = f"({join_char.join(insert_columns)})"

    if wtask.parameters.write_disposition == WriteDisposition.WRITEAPPEND:
        table_operation = "insert into"
    elif wtask.parameters.write_disposition == WriteDisposition.WRITETRUNCATE:
        table_operation = f"""truncate table {wtask.parameters.destination_dataset}.{wtask.parameters.destination_table}; 
insert into"""
    elif wtask.parameters.write_disposition == WriteDisposition.WRITETRANSIENT:
        table_operation = f"create or replace table"
        table_operation_suffix = "as"

    sql.append(
        f"{table_operation} {wtask.parameters.destination_dataset}.{wtask.parameters.destination_table} {table_operation_suffix} "
    )

    tables, frm, where = itemgetter("tables", "from", "where")(
        create_sql_conditions(logger, wtask)
    )

    select = create_sql_select(logger, wtask, tables)
    query_list = [",\n".join(select), "\n".join(frm), "\n".join(where), ";\n"]

    sql.append("\n".join(query_list))
    outp = "\n".join(sql)
    logger.info(f"{pop_stack()} - COMPLETED SUCCESSFULLY".center(100, "-"))
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
    logger.info(f"{pop_stack()} - STARTED".center(100, "-"))

    outp = [f"update {task.target_dataset}.{task.target_table} trg"]
    st = []
    pad = max([len(f.name) for f in task.source_to_target])

    for i, f in enumerate(task.source_to_target):
        prefix = "   set " if i == 0 else "       "
        source_name = (
            f.source_name
            if f.source_name
            else f"{task.source_dataset}.{task.source_table}"
        )
        source_column = "" if not f.source_column else f.source_column

        if f.source_column:
            source = f"{task.tables[source_name]}.{source_column}"
        else:
            transformation = f.transformation
            for key in task.tables.keys():
                transformation = transformation.replace(key, task.tables[key])

            source = transformation
        target_table = f"{task.target_dataset}.{task.target_table}"
        target = f"{task.tables[target_table]}.{f.name.ljust(pad)}"

        st.append(f"{prefix}{target} = {source}")

    outp.append(",\n".join(st))
    source_table = f"{task.source_dataset}.{task.source_table}"
    outp.append(f"  from {source_table} {task.tables[source_table]}")

    outp.append("\n".join(create_sql_where(logger, task.where, task.tables)))
    outp.append(";\n")
    logger.info(f"{pop_stack()} - COMPLETED SUCCESSFULLY".center(100, "-"))
    return "\n".join(outp)


def create_sql_select(logger: ILogger, task: SQLTask, tables: dict) -> str:
    """
    > This function creates a list of select statements from the source_to_target list in the SQLTask

    Args:
      logger (ILogger): ILogger - the logger object
      task (SQLTask): SQLTask object
      tables (dict): a dictionary of the tables in the query, keyed by the table name

    Returns:
      A list of strings.
    """

    logger.info(f"{pop_stack()} - STARTED".center(100, "-"))
    logger.debug(
        f"""{pop_stack()} - creating select list from
                               task - {task}"""
    )
    select = []
    # for each column in the source_to_target we identify the source table and column,
    # or where there is transformation use that in place of the source table and column,
    # and target column.
    for i, column in enumerate(task.parameters.source_to_target):
        prefix = "select " if i == 0 else "       "

        source = column.source(task.parameters.driving_table)
        for key in tables.keys():
            source = source.replace(key, tables[key])

        alias = (
            column.name.rjust(
                max(
                    (60 - len(f"{prefix}{source}") + len(column.name)) - 1,
                    1 + len(column.name),
                )
            )
            if column.name != column.source_column
            else ""
        )

        select.append(f"{prefix}{source}{alias}")

    logger.info(f"{pop_stack()} - COMPLETED SUCCESSFULLY".center(100, "-"))
    return select


def create_sql_conditions(logger: ILogger, task: SQLTask) -> dict:
    """
    It takes a SQLTask object and returns a dictionary containing the tables, from and where clauses of
    the SQL statement

    Args:
      logger (ILogger): ILogger - this is the logger object that is passed to the function.
      task (SQLTask): SQLTask object

    Returns:
      A dictionary with the following keys:
        tables: a dictionary of tables and their aliases
        from: a list of strings that represent the from clause
        where: a string that represents the where clause
    """
    logger.info(f"{pop_stack()} - STARTED".center(100, "-"))
    tables = {task.parameters.driving_table: "a"}
    i = 1
    frm = [
        f"  from {task.parameters.driving_table} {tables[task.parameters.driving_table]}"
    ]
    logger.info(f"{pop_stack()} - identifying join conditions")
    if task.parameters.joins:
        for join in task.parameters.joins:

            if join.left:
                if not join.left in tables.keys():
                    tables[join.left] = chr(i + 97)
                    i = +1

            if not join.right in tables.keys():
                tables[join.right] = chr(i + 97)
                i = +1

            frm.append(
                f"{join.join_type.value.rjust(6)} join {join.right} {tables[join.right]}"
            )

            for c in join.on:
                for i, f in enumerate(c.fields):
                    m = re.search(pattern, f, re.IGNORECASE)
                    if m.group("table"):
                        c.fields[i] = f"{tables[m.group('table')]}.{m.group('column')}"

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
                frm.append(
                    f"{prefix}{on_prefix}{left.ljust(pad)} {condition.operator.value} {right}{on_suffix}"
                )

    where = (
        create_sql_where(
            logger,
            task.parameters.where,
            tables,
        )
        if task.parameters.where
        else ""
    )

    outp = {"tables": tables, "from": frm, "where": where}

    logger.info(f"{pop_stack()} - COMPLETED SUCCESSFULLY".center(100, "-"))
    return outp


def create_sql_where(
    logger: ILogger, conditions: list[Condition], tables: dict = {}
) -> str:
    """
    It takes a list of conditions and a dictionary of tables and returns a list of strings that can be
    used in a SQL where clause

    Args:
      logger (ILogger): ILogger - this is the logger object that is passed to the function.
      conditions (list[Condition]): list of Condition objects
      tables (dict): dict = {}

    Returns:
      A list of strings
    """
    logger.info(f"{pop_stack()} - STARTED".center(100, "-"))
    logger.debug(
        f"""{pop_stack()} - creating where conditions:)
                               conditions  - {conditions}
                               tables      - {tables}"""
    )

    where = []

    # for each condition, replace table names with alias
    for c in conditions:
        for i, f in enumerate(c.fields):
            m = re.search(pattern, f, re.IGNORECASE)
            if m.group("table"):
                c.fields[i] = f"{tables[m.group('table')]}.{m.group('column')}"

    pad = max([len(c.fields[0]) for c in conditions]) if len(conditions) > 0 else 0
    for i, condition in enumerate(conditions):
        if i == 0:
            prefix = " where "
        elif condition.condition.value:
            prefix = f"{condition.condition.value.rjust(6)} "
        else:
            prefix = "   and "

        left_table = condition.fields[0]
        right_table = condition.fields[1]

        where.append(
            f"{prefix}{left_table.ljust(pad)} {condition.operator.value} {right_table}"
        )

    logger.info(f"{pop_stack()} - COMPLETED SUCCESSFULLY".center(100, "-"))
    return where
