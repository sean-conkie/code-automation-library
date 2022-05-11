import argparse
import os
import re
import sys
import traceback

from datetime import datetime
from jinja2 import Environment, FileSystemLoader
from lib.jsonhelper import get_json
from lib.logger import ILogger, pop_stack
from lib.sql_helper import create_sql_file


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
    logger.info(f"{pop_stack()} - creating config list")
    if not os.path.isdir(dpath) and os.path.exists(dpath):
        config_list.append(dpath)
    else:
        for filename in os.listdir(dpath):
            logger.debug(f"filename: {filename}")
            m = re.search(r"^cfg_.*\.json$", filename, re.IGNORECASE)
            if m:
                config_list.append(filename)

    # for each config file identified use the content of the JSON to create
    # the python statements needed to be inserted into the template
    for config in config_list:
        path = config if os.path.exists(config) else f"{dpath}{config}"
        cfg = get_json(logger, path)
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
                # for each task, add a new one to cfg["tasks"] with data check tasks.
                if (
                    "block_data_check" not in task["parameters"].keys()
                    or not task["parameters"]["block_data_check"]
                ):
                    data_check_tasks = create_data_check_tasks(
                        logger, task, cfg["properties"]
                    )
                    for t in data_check_tasks:
                        if not t in cfg["tasks"]:
                            cfg["tasks"].append(t)

                task["parameters"] = create_table_task(logger, task, cfg["properties"])
                task["operator"] = "BigQueryOperator"

            elif task["operator"] == "TruncateTable":
                task["parameters"] = create_table_task(logger, task, cfg["properties"])
                task["operator"] = "BigQueryOperator"

            elif task["operator"] == "DataCheck":
                task["operator"] = "BigQueryCheckOperator"

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


def create_data_check_tasks(logger: ILogger, task: dict, properties: dict) -> list:
    """
    This function creates a list of data check tasks for a given table

    Args:
      logger (ILogger): ILogger - this is the logger object that is passed to the function.
      task (dict): the task object from the config file
      properties (dict): a dictionary of properties that are used to create the DAG.

    Returns:
      A list of dictionaries which represent the tasks.
    """
    logger.info(f"{pop_stack()} - STARTED".center(100, "-"))
    data_check_tasks = []

    if "source_to_target" in task["parameters"].keys():
        table_keys = [
            field["name"]
            for field in task["parameters"]["source_to_target"]
            if "pk" in field.keys() and field["pk"] == True
        ]

        history_keys = [
            field["name"]
            for field in task["parameters"]["source_to_target"]
            if "hk" in field.keys() and field["hk"] == True
        ]
    else:
        table_keys = []
        history_keys = []

    logger.info(f"{pop_stack()} - creating row count check")
    dataset = (
        task["parameters"]["destination_dataset"]
        if "destination_dataset" in task["parameters"].keys()
        else properties["dataset_publish"]
    )
    table = task["parameters"]["destination_table"]
    row_count_check_task = {
        "task_id": f"{task['parameters']['destination_table']}_data_check_row_count",
        "operator": "DataCheck",
        "parameters": {"sql": f"select count(*) from {dataset}.{table}"},
        "dependencies": [f"{task['task_id']}"],
    }
    data_check_tasks.append(row_count_check_task)

    # create task to check for duplicates on primary key - if primary key
    # fields specified in config.
    if len(table_keys) > 0:
        logger.info(f"{pop_stack()} - creating duplicate data check")
        dupe_check_task = {
            "task_id": f"{task['parameters']['destination_table']}_data_check_duplicate_records",
            "operator": "DataCheck",
            "parameters": {
                "sql": "sql/data_check_duplicate_records.sql",
                "params": {
                    "DATASET_ID": f"{task['parameters']['destination_dataset']}"
                    if "destination_dataset" in task["parameters"].keys()
                    else f"{properties['dataset_publish']}",
                    "FROM": f"{task['parameters']['destination_table']}",
                    "KEY": f"{', '.join(table_keys)}",
                },
            },
            "dependencies": [task["task_id"]],
        }
        data_check_tasks.append(dupe_check_task)

    # create task to check for multiple open records - if primary key
    # fields specified in config.
    if len(table_keys) > 0 and task["parameters"]["target_type"] == 2:
        logger.info(f"{pop_stack()} - creating duplicate active history data check")
        dates = [
            "effective_from_dt",
            "effective_from_dt_csn_seq",
            "effective_from_dt_seq",
            "effective_to_dt",
        ]
        dupe_check_task = {
            "task_id": f"{task['parameters']['destination_table']}_data_check_open_history_items",
            "operator": "DataCheck",
            "parameters": {
                "sql": "sql/data_check_open_history_items.sql",
                "params": {
                    "DATASET_ID": f"{task['parameters']['destination_dataset']}"
                    if "destination_dataset" in task["parameters"].keys()
                    else f"{properties['dataset_publish']}",
                    "FROM": f"{task['parameters']['destination_table']}",
                    "KEY": f"{', '.join(history_keys)}",
                },
            },
            "dependencies": [task["task_id"]],
        }
        data_check_tasks.append(dupe_check_task)

    logger.info(f"dag files COMPLETED SUCCESSFULLY".center(100, "-"))
    return data_check_tasks


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

    logger.info(f"{pop_stack()} - STARTED".center(100, "-"))
    dataset_staging = properties["dataset_staging"]
    dataset_publish = (
        "{dataset_publish}"
        if not "destination_dataset" in task["parameters"].keys()
        else task["parameters"]["destination_dataset"]
    )  # set to use variable from target file if not in task parameters
    destination_dataset_table = (
        f"{dataset_publish}.{task['parameters']['destination_table']}"
    )

    # if user has provided a link to a .sql file, use it otherwise look to create .sql from source to target parameter
    if "sql" in task["parameters"].keys():
        sql = task["parameters"]["sql"]
    else:
        file_path = create_sql_file(logger, task, dataset_staging=dataset_staging)
        sql = f"{file_path.replace('./','')}"

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
        "params": {"dataset_publish": dataset_publish},
    }

    logger.info(f"{pop_stack()} - COMPLETED SUCCESSFULLY".center(100, "-"))
    return outp


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
        elif key == "params":
            # value = f"{{'dataset_publish': f'{task['parameters'][key]['dataset_publish']}'}}"
            value = {}
            for p in task["parameters"]["params"].keys():
                value[p] = (
                    "f'{dataset_publish}'"
                    if task["parameters"][key][p] == "{dataset_publish}"
                    else f'{task["parameters"]["params"][p]}'
                )

        else:
            value = f"{task['parameters'][key]}"

        outp.append(f"{key} = {value}")
    outp.append("dag=dag)")

    logger.info(f"{pop_stack()} - COMPLETED SUCCESSFULLY".center(100, "-"))
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
    logger.info(f"{pop_stack()} - STARTED".center(100, "-"))
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

    logger.info(f"{pop_stack()} - COMPLETED SUCCESSFULLY".center(100, "-"))
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
    logger.info(f"{pop_stack()} - STARTED".center(100, "-"))
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

    logger.info(f"{pop_stack()} - COMPLETED SUCCESSFULLY".center(100, "-"))
    return outp


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
        "--sql_destination",
        required=False,
        dest="sql_output_directory",
        default="./dags/sql/",
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

    log_file_name = os.path.normpath(
        f'./logs/builddags_{datetime.now().strftime("%Y-%m-%dT%H%M%S")}.log'
    )
    logger = ILogger("builddags", log_file_name, known_args.level)

    try:
        main(logger, known_args)
    except:
        logger.error(f"{traceback.format_exc():}")
        logger.debug(f"{sys.exc_info()[1]:}")
        logger.info(f"dag files FAILED".center(100, "-"))
