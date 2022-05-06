import re

from datetime import datetime, timedelta
from jinja2 import Environment, FileSystemLoader
from logger import ILogger, pop_stack


def create_sql_file(
    logger: ILogger,
    task: dict,
    file_path: str = "./dags/sql/",
    dataset_staging: str = None,
) -> str:
    """
    > This function creates a SQL file for a given task

    The function takes in the following arguments:

    - logger: This is the logger object that is passed to the function.
    - task: The task dictionary
    - file_path: The path to the directory where the SQL file will be created.
    - dataset_staging: The name of the staging dataset

    Args:
      logger (ILogger): ILogger - this is the logger object that is passed to the function.
      task (dict): the task dictionary
      file_path (str): The path to the directory where the SQL file will be created. Defaults to
    ./dags/sql
      dataset_staging (str): The name of the staging dataset.

    Returns:
      The path to the SQL file that was created.
    """

    logger.info(f"{pop_stack()} - STARTED".center(100, "-"))
    sql = create_sql(logger, task, dataset_staging)
    file_loader = FileSystemLoader("./templates")
    env = Environment(loader=file_loader)

    template = env.get_template("template_sql.txt")
    output = template.render(
        sql=sql,
        task_id=task["task_id"],
        created_date=datetime.now().strftime("%d %b %Y"),
    )

    sql_file = f"{file_path}{task['task_id']}.sql"
    with open(sql_file, "w") as outfile:
        outfile.write(output)

    logger.info(f"{pop_stack()} - COMPLETED SUCCESSFULLY".center(100, "-"))
    return sql_file


def create_sql(logger: ILogger, task: dict, dataset_staging: str = None) -> str:
    """Method for generating a SQL query to be executed by the task.

    This method uses the details supplied in the config to create a string containing
    a SQL query.

    Task contains a parameter table_type allowing the method to generate type 1 or type 2
    logic.

    Calls:
        create_sql_conditions
        create_sql_select
        create_sql_where

    Args:
      task (dict): A dictionary of representing a task to be added to the DAG.  Used to create a task
    parameter string
      dataset_staging (str): The name of the staging dataset.

    Returns:
      A string containing the SQL query to be executed by the task.
    """
    logger.info(f"{pop_stack()} - STARTED".center(100, "-"))

    target_dataset = (
        "{{dataset_publish}}"
        if "destination_dataset" not in task["parameters"].keys()
        else task["parameters"]["destination_dataset"]
    )
    write_disposition = (
        "WRITE_TRUNCATE"
        if not "write_disposition" in task["parameters"].keys()
        else task["parameters"]["write_disposition"]
    )

    logger.info(
        f'{pop_stack()} - creating sql for table type {task["parameters"]["target_type"]}'
    )
    if task["parameters"]["target_type"] == 1:
        sql = create_type_1_sql(logger, task, target_dataset, write_disposition)
    elif task["parameters"]["target_type"] == 2:
        sql = create_type_2_sql(
            logger, task, target_dataset, dataset_staging, write_disposition
        )

    outp = "\n".join(sql)
    logger.info(f"{pop_stack()} - COMPLETED SUCCESSFULLY".center(100, "-"))
    return outp


def create_type_1_sql(
    logger: ILogger, task: dict, target_dataset: str, write_disposition: str
) -> list:
    """
    It creates a SQL statement that will insert data into a table

    Args:
      logger (ILogger): ILogger - this is the logger object that is passed to the function.
      task (dict): the task dictionary
      target_dataset (str): The dataset where the destination table is located.
      write_disposition (str): This is the write disposition for the query. It can be WRITE_TRUNCATE,
    WRITE_APPEND, WRITE_EMPTY.

    Returns:
      A list of strings
    """
    logger.info(f"{pop_stack()} - STARTED".center(100, "-"))
    sql = []
    logger.info(f'{pop_stack()} - set write disposition - "{write_disposition}"')
    if write_disposition == "WRITE_TRUNCATE":
        sql.append(
            f"truncate table {target_dataset}.{task['parameters']['destination_table']};"
        )

    sql.append(
        f"insert into {target_dataset}.{task['parameters']['destination_table']}"
    )

    r = create_sql_conditions(logger, task)
    tables = r["tables"]
    frm = r["from"]
    where = r["where"]

    select = create_sql_select(logger, task, tables)

    sql.append(",\n".join(select))
    sql.append("\n".join(frm))
    sql.append("\n".join(where))
    sql.append(";\n")
    logger.info(f"{pop_stack()} - COMPLETED SUCCESSFULLY".center(100, "-"))
    return sql


def create_type_2_sql(
    logger: ILogger,
    task: dict,
    target_dataset: str,
    dataset_staging: str,
    write_disposition: str,
) -> list:
    """
    > Create a series of SQL statements to create a table that contains the current and previous values
    of the driving columns, then create a table that contains the current and previous values of the
    driving columns and the current and previous values of the other columns, then create a table that
    contains the current and previous values of the driving columns and the current and previous values
    of the other columns and the effective_to_dt column

    Args:
      logger (ILogger): ILogger - this is the logger object that is passed to the function
      task (dict): the task object from the config file
      target_dataset (str): The dataset where the table will be created
      dataset_staging (str): the dataset where the transient tables will be created
      write_disposition (str): This is the write disposition for the final table.  This can be
    WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY.  If you choose WRITE_APPEND, you will need to add a where
    clause to the final query to ensure you don't duplicate data.

    Returns:
      A list of SQL statements
    """
    logger.info(f"{pop_stack()} - STARTED".center(100, "-"))
    sql = []
    td_table = re.sub(r"^[a-zA-Z]+_", "td_", task["parameters"]["destination_table"])
    logger.info(
        f'{pop_stack()} - create sql for transient table, pull source data and previous columns - "{dataset_staging}.{td_table}_p1"'
    )

    # first we create p1, this table contains required columns plus previous
    # value for driving tables.  Previous values are used later to complete CDC
    analytics = create_type_2_analytic_list(logger, task)
    sql.append(
        create_table_query(
            logger,
            task,
            dataset_staging,
            f"{td_table}_p1",
            dataset_staging,
            "WRITE_TRANSIENT",
            analytics,
        )
    )

    logger.info(
        f'{pop_stack()} - create sql for transient table, complete CDC - "{dataset_staging}.{td_table}_p2"'
    )

    # second we complete CDC.  We create a new task object using our p1 table as
    # driving table
    p2_task = {
        "parameters": {
            "write_disposition": "WRITE_TRUNCATE",
            "driving_table": f"{dataset_staging}.{td_table}_p1",
            "source_to_target": [
                {
                    "transformation": f"* except({','.join([t['column']['name'] for t in analytics])})"
                }
            ],
            "where": [
                {
                    "fields": [
                        f"ifnull(cast({c['name']} as string),'NULL')",
                        f"ifnull(cast(prev_{c['name']} as string),'NULL')",
                    ],
                    "operator": "!=",
                    "condition": "or",
                }
                for c in task["parameters"]["history"]["driving_column"]
            ],
        }
    }

    sql.append(
        create_table_query(
            logger,
            p2_task,
            dataset_staging,
            f"{td_table}_p2",
            dataset_staging,
            "WRITE_TRANSIENT",
        )
    )

    logger.info(
        f'{pop_stack()} - create sql for transient table, add/replace effective_to_dt with lead - "{dataset_staging}.{td_table}"'
    )
    # final step takes the data following the CDC and adds the effective_to_dt
    td_task = {
        "parameters": {
            "write_disposition": "WRITE_TRUNCATE",
            "driving_table": f"{dataset_staging}.{td_table}_p2",
            "source_to_target": [
                {"name": c["name"], "source_column": c["name"]}
                for c in task["parameters"]["source_to_target"]
            ],
        }
    }
    analytics = [
        {
            "type": "lead",
            "column": {
                "name": f"effective_to_dt",
                "source_column": "effective_from_dt",
            },
            "partition": [
                {"name": f"{p['name']}", "source_column": f"{p['name']}"}
                for p in task["parameters"]["history"]["partition"]
            ],
            "order": [
                {"name": "effective_from_dt", "source_column": "effective_from_dt"},
                {
                    "name": "effective_from_dt_csn_seq",
                    "source_column": "effective_from_dt_csn_seq",
                },
                {
                    "name": "effective_from_dt_seq",
                    "source_column": "effective_from_dt_seq",
                },
            ],
            "offset": ", 1",
            "default": ", timestamp('2999-12-31 23:59:59')",
        }
    ]
    sql.append(
        create_table_query(
            logger,
            td_task,
            target_dataset,
            f"{td_table}",
            dataset_staging,
            write_disposition,
            analytics,
        )
    )

    logger.info(f"{pop_stack()} - COMPLETED SUCCESSFULLY".center(100, "-"))
    return sql


def create_type_2_analytic_list(logger: ILogger, task: dict) -> list:
    """
    > This function creates a list of analytic functions that will be used to create a lag column for
    each column in the driving column list

    Args:
      logger (ILogger): ILogger - this is the logger object that is passed to the function.
      task (dict): the task dictionary

    Returns:
      A list of dictionaries.
    """
    logger.info(f"{pop_stack()} - STARTED".center(100, "-"))
    outp = []
    history = (
        task["parameters"]["history"] if "history" in task["parameters"].keys() else []
    )
    for c in history["driving_column"]:
        analytic = {
            "type": "lag",
            "column": {
                "name": f"prev_{c['name']}",
                "source_column": c["source_column"],
            },
            "partition": history["partition"],
            "order": history["order"],
            "offset": ", 1",
            "default": "",
        }
        outp.append(analytic)

    logger.info(f"{pop_stack()} - COMPLETED SUCCESSFULLY".center(100, "-"))
    return outp


def create_table_query(
    logger: ILogger,
    task: dict,
    target_dataset: str,
    target_table: str,
    dataset_staging: str,
    write_disposition: str,
    analytics: list = [],
) -> str:
    """
    This function creates a SQL query to create a table in BigQuery

    Args:
      logger (ILogger): ILogger - this is the logger object that is passed to the function
      task (dict): the task dictionary
      target_dataset (str): The dataset where the target table resides.
      target_table (str): The name of the table to be created.
      dataset_staging (str): the name of the staging dataset
      write_disposition (str): This is the write disposition for the query. It can be WRITE_TRUNCATE,
    WRITE_APPEND, WRITE_EMPTY, WRITE_TRANSIENT.
      analytics (list): list = []) -> str: list of analytic dict objects

    Returns:
      A string of SQL
    """
    logger.info(f"{pop_stack()} - STARTED".center(100, "-"))
    sql = []
    # create working task object so it can be edited without impacting
    # original
    wtask = {
        "parameters": {
            "destination_table": task["parameters"]["destination_table"]
            if "desitnation_table" in task["parameters"].keys()
            else "",
            "driving_table": task["parameters"]["driving_table"],
            "source_to_target": [c for c in task["parameters"]["source_to_target"]],
        }
    }

    if "joins" in task["parameters"].keys():
        wtask["parameters"]["joins"] = [j for j in task["parameters"]["joins"]]

    if "where" in task["parameters"].keys():
        wtask["parameters"]["where"] = [w for w in task["parameters"]["where"]]

    if write_disposition == "WRITE_APPEND":
        table_operation = "insert into"
        table_operation_suffix = ""
    elif write_disposition == "WRITE_TRUNCATE":
        table_operation = f"""truncate table {target_dataset}.{target_table}; 
insert into"""
        table_operation_suffix = ""
    elif write_disposition == "WRITE_TRANSIENT":
        table_operation = f"create or replace"
        table_operation_suffix = "as"

    sql.append(
        f"{table_operation} table {target_dataset}.{target_table} {table_operation_suffix} "
    )

    r = create_sql_conditions(logger, wtask)
    tables = r["tables"]
    frm = r["from"]
    where = r["where"]

    logger.info(f"{pop_stack()} - create analytic transformations")

    # for each analytic provided identify the partition fields, order by
    # fields and create the function statement
    for analytic in analytics:
        partition_list = []
        for p in analytic["partition"]:
            source_name = (
                p["source_name"]
                if "source_name" in p.keys()
                else task["parameters"]["driving_table"]
            )
            source_column = p["source_column"] if "source_column" in p.keys() else ""
            partition_list.append(
                f"{source_name}.{source_column}"
                if "source_column" in p.keys()
                else f'{p["transformation"]}'
            )
        partition = ",".join(partition_list)

        order_list = []
        for p in analytic["order"]:
            source_name = (
                p["source_name"]
                if "source_name" in p.keys()
                else task["parameters"]["driving_table"]
            )
            source_column = p["source_column"] if "source_column" in p.keys() else ""
            order_list.append(
                f"{source_name}.{source_column}"
                if "source_column" in p.keys()
                else f'{p["transformation"]}'
            )
        order = ",".join(order_list)

        source_name = (
            analytic["column"]["source_name"]
            if "source_name" in analytic["column"].keys()
            else task["parameters"]["driving_table"]
        )

        analytic_transformation = {
            "name": analytic["column"]["name"],
            "transformation": f"{analytic['type']}({source_name}.{source_column}{analytic['offset']}{analytic['default']}) over(partition by {partition} order by {order})",
        }

        wtask["parameters"]["source_to_target"].append(analytic_transformation)

    select = create_sql_select(logger, wtask, tables)

    sql.append(",\n".join(select))
    sql.append("\n".join(frm))
    sql.append("\n".join(where))
    sql.append(";\n")
    logger.info(f"{pop_stack()} - COMPLETED SUCCESSFULLY".center(100, "-"))
    return "\n".join(sql)


def create_sql_select(logger: ILogger, task: dict, tables: dict) -> str:
    """Method for generating the select part of the SQL query.

    Uses the columns supplied in the source_to_target array to create the select statement,
    columns are aliased based on the tables dictionary which is created by create_sql_conditions
    method.

    args:
        task: A dictionary of representing a task to be added to the DAG.  Used to
              create a task parameter string
        tables: Dictionary containing all tables related to the query and an alias

    returns:
        A string which can be used a the select part of the SQL query.
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
    for i, column in enumerate(task["parameters"]["source_to_target"]):
        prefix = "select " if i == 0 else "       "
        source_name = (
            task["parameters"]["driving_table"]
            if not "source_name" in column.keys()
            else column["source_name"]
        )
        source_column = (
            "" if not "source_column" in column.keys() else column["source_column"]
        )
        if "source_column" in column.keys():
            source = f"{tables[source_name]}.{source_column}"
        else:
            transformation = column["transformation"]
            for key in tables.keys():
                transformation = transformation.replace(key, tables[key])

            source = transformation

        alias = (
            column["name"].rjust(
                max(
                    (60 - len(f"{prefix}{source}") + len(column["name"])) - 1,
                    1 + len(column["name"]),
                )
            )
            if "name" in column.keys() and not column["name"] == source_column
            else ""
        )

        select.append(f"{prefix}{source}{alias}")

    logger.info(f"{pop_stack()} - COMPLETED SUCCESSFULLY".center(100, "-"))
    return select


def create_sql_conditions(logger: ILogger, task: dict) -> dict:
    """Method for generating the conditions for the SQL query.

    This method uses the details supplied in the config to identify all tables used,
    to create SQL for any joins and calls create_sql_where for the where clause(s).

    args:
        task: A dictionary of representing a task to be added to the DAG.  Used to
              create a task parameter string


    returns:
        A dictionary containing the tables dict, from statement and where clause for the sql
        outp:
            tables: Dictionary containing all tables related to the query and an alias
            from: a string with the SQL from and join(s)
            where: a string containing any where conditions
    """

    logger.info(f"{pop_stack()} - STARTED".center(100, "-"))
    tables = {task["parameters"]["driving_table"]: "a"}
    i = 1
    frm = [
        f"  from {task['parameters']['driving_table']} {tables[task['parameters']['driving_table']]}"
    ]
    logger.info(f"{pop_stack()} - identifying join conditions")
    if "joins" in task["parameters"].keys():
        for join in task["parameters"]["joins"]:
            left_table = ""
            right_table = ""

            if "left" in join.keys():
                if not join["left"] in tables.keys():
                    tables[join["right"]] = chr(i + 97)
                    i = +1
            if not join["right"] in tables.keys():
                tables[join["right"]] = chr(i + 97)
                i = +1

            join_type = "left" if not "type" in join.keys() else join["type"]
            left_table = (
                task["parameters"]["driving_table"]
                if not "left" in join.keys()
                else join["left"]
            )
            right_table = join["right"]
            frm.append(
                f"{join_type.rjust(6)} join {join['right']} {tables[join['right']]}"
            )
            for j, condition in enumerate(join["on"]):
                on_prefix = "(    " if len(join["on"]) > 1 and j == 0 else ""
                on_suffix = ")" if len(join["on"]) == j + 1 else ""
                prefix = "    on " if j == 0 else "        and "

                left = f"{condition['fields'][0].replace(left_table,f'{tables[left_table]}').replace(right_table,f'{tables[right_table]}')}"
                right = f"{condition['fields'][1].replace(left_table,f'{tables[left_table]}').replace(right_table,f'{tables[right_table]}')}"
                frm.append(
                    f"{prefix}{on_prefix}{left} {condition['operator']} {right}{on_suffix}"
                )
    else:
        left_table = task["parameters"]["driving_table"]
        right_table = ""

    where = (
        create_sql_where(logger, task["parameters"]["where"], tables)
        if "where" in task["parameters"].keys()
        else ""
    )

    outp = {"tables": tables, "from": frm, "where": where}

    logger.info(f"{pop_stack()} - COMPLETED SUCCESSFULLY".center(100, "-"))
    return outp


def create_sql_where(logger: ILogger, conditions: list, tables: dict = {}) -> str:
    """Method for generating the where conditions of the SQL query.

    Uses the wehere object of the task to create the string.

    args:
        conditions: list of dictionaries, each item contains the condition operator and the field(s) and/or value(s)
        tables: Dictionary containing all tables related to the query and an alias

    returns:
        A string which can be used a the where conditions of the SQL query.
    """
    logger.info(f"{pop_stack()} - STARTED".center(100, "-"))
    logger.debug(
        f"""{pop_stack()} - creating where conditions:)
                               conditions  - {conditions}
                               tables      - {tables}"""
    )

    where = []
    for i, condition in enumerate(conditions):
        if i == 0:
            prefix = " where "
        elif "condition" in condition.keys():
            prefix = f"{condition['condition'].rjust(6)} "
        else:
            prefix = "   and "

        left_table_list = condition["fields"][0].split(".")
        right_table_list = condition["fields"][1].split(".")

        left_table = (
            f"{left_table_list[0]}.{left_table_list[1]}"
            if len(left_table_list) > 1
            else ""
        )
        right_table = (
            f"{right_table_list[0]}.{right_table_list[1]}"
            if len(right_table_list) > 1
            else ""
        )

        left = f"{condition['fields'][0].replace(left_table,f'{tables[left_table] if left_table in tables.keys() else left_table}')}"
        right = f"{condition['fields'][1].replace(right_table,f'{tables[right_table] if right_table in tables.keys() else right_table}')}"
        where.append(f"{prefix}{left} {condition['operator']} {right}")

    logger.info(f"{pop_stack()} - COMPLETED SUCCESSFULLY".center(100, "-"))
    return where


def create_task(logger: ILogger, task: dict) -> str:
    """Method for generating a string of python that defines a task.

    args:
        task: A dictionary representing a task to be added to the DAG.  Used to
              task parameter string

    returns:
        A string of python code that can be added to the target file
    """
    logger.info(f"{pop_stack()} - STARTED".center(100, "-"))
    logger.debug(
        f"""{pop_stack()} - creating task {task["task_id"]} from:
                               parameters - {task["parameters"]}"""
    )

    outp = [f"{task['task_id']} = {task['operator']}(task_id='{task['task_id']}'"]

    # for each key:value pair in the tark parameters we perform checks based on
    # parameter type and create a value that can be appended to the string
    for key in task["parameters"].keys():
        if (
            type(task["parameters"][key]) == int
            or type(task["parameters"][key]) == bool
        ):
            value = task["parameters"][key]
        elif type(task["parameters"][key]) == str:
            value = f"f'''{task['parameters'][key]}'''"
        else:
            value = f"{task['parameters'][key]}"

        outp.append(f"{key} = {value}")
    outp.append("dag=dag)")

    logger.info(f"{pop_stack()} - COMPLETED SUCCESSFULLY".center(100, "-"))
    return ",\n          ".join(outp)
