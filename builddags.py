import argparse
import inspect
import json
import os
import re
import sys
import traceback

from datetime import datetime, timedelta
from jinja2 import Environment, FileSystemLoader
from logger import ILogger


def main(logger: ILogger, args: argparse.Namespace) -> int:
    """Main method for generating .py files containing DAGs from config files.

    From JSON config file(s) supplied as a dir or file, generate a .py file
    using template_dag.txt.  Loops through all configs provided and creates
    one .py file per config.

    args:
        args: A dictionary of input parameters, defaults to empty dictionary.
              Dictionary can contain one or more key value paris of;
                --dir: source file path or directory containing config files
                --out-dir: target output file path for .py files to be saved

    returns:
        Saves .py files in output directory and returns;
            0: succesful
            1: failure
    """

    logger.info(f"dag files STARTED".center(100, "-"))
    dpath = args.config_directory
    opath = args.output_directory
    config_list = []

    # create a list of config files using the source directory (dpath), if the
    # path provided is a file add id otherwise append each filename in directory
    try:
        logger.info(f"{pop_stack()} - creating config list")
        if not os.path.isdir(dpath) and os.path.exists(dpath):
            config_list.append(dpath)
        else:
            for filename in os.listdir(dpath):
                logger.debug(f"filename: {filename}")
                m = re.search(r"^cfg_.*\.json$", filename, re.IGNORECASE)
                if m:
                    config_list.append(filename)
    except:
        logger.error(f"{pop_stack()} - {sys.exc_info()[0]:}")
        logger.info(f"{pop_stack()} - dag files FAILED")
        return 1

    # for each config file identified use the content of the JSON to create
    # the python statements needed to be inserted into the template
    for config in config_list:
        path = config if os.path.exists(config) else f"{dpath}{config}"
        cfg = get_config(logger, path)
        logger.info(f"{pop_stack()} - building dag - {cfg['name']}")

        dag_string = create_dag_string(logger, cfg["name"], cfg["dag"])
        default_args = create_dag_args(logger, cfg["args"])
        imports = "\n".join(cfg["imports"])
        tasks = []
        dependencies = []

        # for each item in the task array, check the operator type and use this
        # to determine the task parameters to be used
        for task in cfg["tasks"]:
            logger.info(f'{pop_stack()} - creating task "{task["task_id"]}"')
            if task["operator"] == "CreateTable":
                task["parameters"] = create_table_task(logger, task, cfg["properties"])
                task["operator"] = "BigQueryOperator"

            tasks.append(create_task(logger, task))

            if "dependencies" in task.keys():
                if len(task["dependencies"]) > 0:
                    # for each entry in the dependencies array, add the item as a dependency.
                    # where the dependency is on an external task, create an external task if
                    # no task already exists
                    for dep in task["dependencies"]:
                        dep_list = dep.split(".")
                        if len(dep_list) > 1:
                            dep_task = f"ext_{dep_list[1]}"
                            if not dep_task in [t.split(" ")[0].strip() for t in tasks]:
                                ext_task = {
                                    "task_id": f"{dep_task}",
                                    "operator": "ExternalTaskSensor",
                                    "parameters": {
                                        "external_dag_id": dep_list[0],
                                        "external_task_id": dep_list[1],
                                        "check_existence": True,
                                        "timeout": 600,
                                        "allowed_states": ["success"],
                                        "failed_states": ["failed", "skipped"],
                                        "mode": "reschedule",
                                    },
                                }
                                tasks.append(create_task(logger, ext_task))
                                dependencies.append(f"start_pipeline >> {dep_task}")
                        else:
                            dep_task = dep
                        dependencies.append(f"{dep_task} >> {task['task_id']}")
                else:
                    dependencies.append(f"start_pipeline >> {task['task_id']}")

        dep_tasks = [d[0].strip() for d in [dep.split(">") for dep in dependencies]]
        final_tasks = [
            task["task_id"] for task in cfg["tasks"] if not task["task_id"] in dep_tasks
        ]

        for task in final_tasks:
            dependencies.append(f"{task} >> finish_pipeline")

        properties = [
            f"{key} = '{cfg['properties'][key]}'" for key in cfg["properties"].keys()
        ]

        logger.info(f"{pop_stack()} - populating template")
        file_loader = FileSystemLoader("./templates")
        env = Environment(loader=file_loader)

        template = env.get_template("template_dag.txt")
        output = template.render(
            imports=imports,
            tasks=tasks,
            default_args=default_args,
            dag_string=dag_string,
            dependencies=dependencies,
            properties=properties,
        )

        dag_file = f"{opath}{cfg['name']}.py"
        with open(dag_file, "w") as outfile:
            outfile.write(output)

    logger.info(f"dag files COMPLETED SUCCESSFULLY".center(100, "-"))
    return 0


def create_table_task(logger: ILogger, task: dict, properties: dict) -> dict:
    """Method for generating parameters dictionary for standard create table sql.

    Uses the task object to populate the required parameters for the BigQueryOperator.

    args:
        task: A dictionary of representing a task to be added to the DAG.  Used to
              create a task parameter string
        properties: DAG properties.  Used to obtain DAG level properties, such as
                    the staging dataset

    returns:
        A dictionary containing expected parameters for the desired task (BigQueryOperator)
    """

    logger.info(f"{pop_stack()} - STARTED")
    dataset_staging = properties["dataset_staging"]
    dataset_publish = (
        "{dataset_publish}"
        if not "destination_dataset" in task["parameters"].keys()
        else task["parameters"]["destination_dataset"]
    )  # set to use variable from target file if not in task parameters
    destination_dataset_table = (
        f"{dataset_publish}.{task['parameters']['destination_table']}"
    )
    sql = (
        task["parameters"]["sql"]
        if "sql" in task["parameters"].keys()
        else f"{create_sql(logger, task, dataset_staging)}"
    )  # if user has provided a link to a .sql file, use it otherwise look to create sql from source to target parameter
    write_disposition = (
        "WRITE_TRUNCATE"
        if not "write_disposition" in task["parameters"].keys()
        else f"{task['parameters']['write_disposition']}"
    )

    outp = {
        "sql": sql,
        "destination_dataset_table": destination_dataset_table,
        "write_disposition": write_disposition,
        "create_disposition": "CREATE_IF_NEEDED",
        "allow_large_results": True,
        "use_legacy_sql": False,
    }

    logger.info(f"{pop_stack()} - COMPLETED SUCCESSFULLY")
    return outp


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
    logger.info(f"{pop_stack()} - STARTED")
    sql = []
    target_dataset = (
        "{dataset_publish}"
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

    elif task["parameters"]["target_type"] == 2:
        td_table = re.sub(
            r"^[a-zA-Z]+_", "td_", task["parameters"]["destination_table"]
        )
        logger.info(
            f'{pop_stack()} - create sql for transient table, pull source data and previous columns - "{dataset_staging}.{td_table}_p1"'
        )
        sql.append(f"create or replace table {dataset_staging}.{td_table}_p1 as")

        r = create_sql_conditions(logger, task)
        tables = r["tables"]
        frm = r["from"]
        where = r["where"]

        select = create_sql_select(logger, task, tables)

        history = task["parameters"]["history"]
        logger.info(f"{pop_stack()} - setting history parameters")
        partition_list = []
        for p in history["partition"]:
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
        for p in history["order"]:
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

        logger.info(
            f"{pop_stack()} - add previous fields for {history['driving_column']}"
        )
        prev_task = {
            "parameters": {
                "source_to_target": [],
                "driving_table": task["parameters"]["driving_table"],
            }
        }
        prev_conditions = []
        for col in history["driving_column"]:
            source_name = (
                col["source_name"]
                if "source_name" in col.keys()
                else task["parameters"]["driving_table"]
            )
            source_column = (
                col["source_column"] if "source_column" in col.keys() else ""
            )
            cj = {
                "name": f"prev_{col['name']}",
                "transformation": f"lag({source_name}.{source_column},1) over(partition by {partition} order by {order})",
            }
            prev_task["parameters"]["source_to_target"].append(cj)
            prev_conditions.append(
                {
                    "operator": "<>",
                    "fields": [
                        f"ifnull(cast({col['name']} as string),'NULL')",
                        f"ifnull(cast(prev_{col['name']} as string),'NULL')",
                    ],
                }
            )

        prev_select = create_sql_select(logger, prev_task, tables)
        prev_select[0] = prev_select[0].replace("select", "      ")
        for ps in prev_select:
            select.append(ps)

        sql.append(",\n".join(select))
        sql.append("\n".join(frm))
        sql.append("\n".join(where))
        sql.append(";\n")

        logger.info(
            f'{pop_stack()} - create sql for transient table, complete CDC - "{dataset_staging}.{td_table}_p2"'
        )
        sql.append(f"create or replace table {dataset_staging}.{td_table}_p2 as")

        select = f"select * except({','.join([t['name'] for t in prev_task['parameters']['source_to_target']])})"
        tables[f"{dataset_staging}.{td_table}_p1"] = chr(len(tables.keys()) + 97)
        frm = f"  from {dataset_staging}.{td_table}_p1 {tables[f'{dataset_staging}.{td_table}_p1']}"

        where = create_sql_where(logger, prev_conditions)

        sql.append(select)
        sql.append(frm)
        sql.append("\n".join(where))
        sql.append(";\n")

        logger.info(
            f'{pop_stack()} - create sql for transient table, add/replace effective_to_dt with lead - "{dataset_staging}.{td_table}"'
        )

        logger.info(
            f'{pop_stack()} - set write disposition - "{task["parameters"]["write_disposition"]}"'
        )

        if task["parameters"]["write_disposition"] == "WRITE_TRUNCATE":
            sql.append(
                f"truncate table {target_dataset}.{task['parameters']['destination_table']};"
            )

        sql.append(
            f"insert into {target_dataset}.{task['parameters']['destination_table']}"
        )

        logger.info(f"{pop_stack()} - re-calculating history parameters")
        partition_list = []
        for p in history["partition"]:
            partition_list.append(f'{p["name"]}')
        partition = ",".join(partition_list)

        order_list = []
        for p in history["order"]:
            order_list.append(f'{p["name"]}')
        order = ",".join(order_list)

        new_source_to_target = {
            "parameters": {
                "source_to_target": [],
                "driving_table": task["parameters"]["driving_table"],
            }
        }
        for s in task["parameters"]["source_to_target"]:
            if s["name"] == "effective_to_dt":
                ns = {
                    "name": s["name"],
                    "transformation": f"lead(effective_from_dt,1,timestamp('2999-12-31 23:59:59')) over(partition by  {partition} order by {order})",
                }
            else:
                ns = {
                    "name": s["name"],
                    "source_name": f"{dataset_staging}.{td_table}_p2",
                    "source_column": s["name"],
                }
            new_source_to_target["parameters"]["source_to_target"].append(ns)

        tables[f"{dataset_staging}.{td_table}_p2"] = chr(len(tables.keys()) + 97)
        select = create_sql_select(logger, new_source_to_target, tables)
        frm = f"  from {dataset_staging}.{td_table}_p2 {tables[f'{dataset_staging}.{td_table}_p2']}"

        sql.append(",\n".join(select))
        sql.append(frm)
        sql.append(";\n")

    outp = "\n".join(sql)
    logger.info(f"{pop_stack()} - COMPLETED SUCCESSFULLY")
    return outp


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

    logger.info(f"{pop_stack()} - STARTED")
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
            if not column["name"] == source_column
            else ""
        )

        select.append(f"{prefix}{source}{alias}")

    logger.info(f"{pop_stack()} - COMPLETED SUCCESSFULLY")
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

    logger.info(f"{pop_stack()} - STARTED")
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

    logger.info(f"{pop_stack()} - COMPLETED SUCCESSFULLY")
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
    logger.info(f"{pop_stack()} - STARTED")
    logger.debug(
        f"""{pop_stack()} - creating where conditions:)
                               conditions  - {conditions}
                               tables      - {tables}"""
    )

    where = []
    for i, condition in enumerate(conditions):
        prefix = " where " if i == 0 else "   and "

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

    logger.info(f"{pop_stack()} - COMPLETED SUCCESSFULLY")
    return where


def create_task(logger: ILogger, task: dict) -> str:
    """Method for generating a string of python that defines a task.

    args:
        task: A dictionary representing a task to be added to the DAG.  Used to
              task parameter string

    returns:
        A string of python code that can be added to the target file
    """
    logger.info(f"{pop_stack()} - STARTED")
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

    logger.info(f"{pop_stack()} - COMPLETED SUCCESSFULLY")
    return ",\n          ".join(outp)


def create_dag_string(logger: ILogger, name: str, dag: dict) -> str:
    """Method for generating a string of python that defines a dag.

    DAG parameters are provided and used to populate a string which can be
    added to the target file.


    Args:
      name (str): The name of the DAG.
      dag (dict): A dictionary representing the DAG.  Used to create dag string

    Returns:
      A string of python code that can be added to the target file
    """
    logger.info(f"{pop_stack()} - STARTED")
    # we first set DAG defaults - these can also be excluded completely and
    # use Environment settings
    odag = {
        "concurrency": 10,
        "max_active_runs": 1,
        "default_args": "default_args",
        "schedule_interval": None,
        "start_date": "datetime.now()",
        "catchup": False,
    }

    for key in dag.keys():
        if key in ["concurrency", "max_active_runs"]:
            if type(dag[key]) == int:
                odag[key] = dag[key]  # only use provided value if it is an int
        elif key in ["catchup"]:
            if type(dag[key]) == bool:
                odag[key] = dag[key]  # only use provided value if it is an bool
        elif (
            key in ["tags"] and not type(dag["tags"]) == list
        ):  # if tags not provided as a list, wrap in list
            odag[key] = [dag[key]]
        else:
            odag[key] = dag[key]

    odag[
        "description"
    ] = f'"{dag["description"] if "description" in dag.keys() else dag["name"]}"'

    outp = f"'{name}',{', '.join([f'{key} = {odag[key]}' for key in odag.keys()])}"

    logger.info(f"{pop_stack()} - COMPLETED SUCCESSFULLY")
    return outp


def create_dag_args(logger: ILogger, args: dict) -> str:
    """
    > This function takes a dictionary of arguments and returns a string that can be used to create a
    DAG in Airflow

    Args:
      args (dict): dict = {

    Returns:
      A string that is a dictionary of arguments for the DAG.
    """
    logger.info(f"{pop_stack()} - STARTED")
    oargs = {
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 5,
        "retry_delay": "timedelta(seconds=60)",
        "queue": "",
        "pool": "",
        "priority_weight": 10,
        "end_date": "",
        "wait_for_downstream": False,
        "sla": "timedelta(seconds=7200)",
        "execution_timeout": "timedelta(seconds=300)",
        "on_failure_callback": "",
        "on_success_callback": "",
        "on_retry_callback": "",
        "sla_miss_callback": "",
        "trigger_rule": "",
    }

    for key in args.keys():
        if key in [
            "depends_on_past",
            "email_on_failure",
            "email_on_retry",
            "wait_for_downstream",
        ]:
            if not type(args[key]) == bool:
                oargs[key] = args[key]
        elif key in ["retry_delay", "sla", "execution_timeout"]:
            if type(args[key]) == int:
                oargs[key] = f"timedelta(seconds={args[key]})"
        elif key in ["email"]:
            emails = ",".join([f"'{a}'" for a in args[key]])
            oargs[key] = f"[{emails}]"
        elif key in ["priority_weight", "retries"]:
            if type(args[key]) == int:
                oargs[key] = f"{args[key]}"
        elif not args[key] == "":
            oargs[key] = f"'{args[key]}'"

    outstr = ", ".join(
        [f"'{key}':  {oargs[key]}" for key in oargs.keys() if not oargs[key] == ""]
    )
    outp = f"{{{outstr}}}"

    logger.info(f"{pop_stack()} - COMPLETED SUCCESSFULLY")
    return outp


def get_config(logger: ILogger, path: str) -> dict:
    """
    This function takes a path to a config file and returns a
    dictionary object

    Args:
      path: The path to the config file you want to read.

    Returns:
      A dictionary object
    """

    logger.info(f"{pop_stack()} - STARTED")
    if not path:
        logger.warning(f"{pop_stack()} - File {path:} does not exist.")
        return {}

    try:
        # identify what path is; dir, file
        if os.path.isdir(path) or not os.path.exists(path):
            raise FileExistsError
    except (FileNotFoundError, FileExistsError) as e:
        logger.error(f"{pop_stack()} - File {path:} does not exist.")
        logger.info(f"{pop_stack()} - FAILED")
        return
    except:
        logger.error(f"{sys.exc_info()[0]:}")
        logger.info(f"{pop_stack()} - FAILED")
        return

    # read file
    try:
        with open(path, "r") as sourcefile:
            filecontent = sourcefile.read()

        # return file
        logger.info(f"{pop_stack()} - COMPLETED SUCCESSFULLY")
        return json.loads(filecontent)
    except:
        logger.error(f"{pop_stack()} - {sys.exc_info()[0]:}")
        logger.info(f"{pop_stack()} - FAILED")
        return


def pop_stack() -> str:
    """
    It returns the name of the file and function that called it

    Returns:
      The name of the file and the function that called the function.
    """
    frame = inspect.stack()[1]
    module = inspect.getmodule(frame[0])
    filename = module.__file__
    return f"file: {os.path.basename(filename)} - method: {frame[3]}"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config_directory",
        required=False,
        dest="config_directory",
        default="./cfg/dag/",
        help="Specify the location of the config file(s) which define the required DAGs. (default: ./cfg)",
    )
    parser.add_argument(
        "--destination",
        required=False,
        dest="output_directory",
        default="./dags/",
        help="Specify the desired output directory for DAGs created. (default: ./dags)",
    )
    parser.add_argument(
        "--log_level",
        required=False,
        dest="level",
        default="DEBUG",
        help="Specify the desired log level (default: DEBUG).  This can be one of the following: 'CRITICAL', 'DEBUG', 'ERROR', 'FATAL','INFO','NOTSET', 'WARNING'",
    )

    known_args, args = parser.parse_known_args()

    log_file_name = f'./logs/builddags_{datetime.now().strftime("%Y-%m-%dT%H%M%S")}.log'
    logger = ILogger("builddags", log_file_name, known_args.level)

    try:
        main(logger, known_args)
    except:
        logger.error(f"{traceback.format_exc():}")
        logger.debug(f"{sys.exc_info()[1]:}")
        logger.info(f"dag files FAILED".center(100, "-"))
