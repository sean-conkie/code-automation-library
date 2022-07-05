import argparse
import black
import os
import pathlib

from lib.baseclasses import (
    TableType,
    TaskOperator,
    Task,
    SQLDataCheckTask,
    SQLDataCheckParameter,
    todict,
)

from jinja2 import Environment, FileSystemLoader
from lib.logger import ILogger, pop_stack
from lib.sql_helper import create_sql_file
from shutil import copy

__all__ = [
    "builddags",
]


def builddags(logger: ILogger, output_directory: str, config: dict) -> int:
    """
    > The function takes a JSON file as input, and creates a DAG file as output

    Args:
      logger (ILogger): ILogger - this is the logger object that is used to log messages to the console
    and to the log file.
      args (argparse.Namespace): argparse.Namespace
      config (dict): The configuration file that contains the DAG definition.

    Returns:
      The return value is the exit code of the function.
    """

    logger.info(f"dag files - {pop_stack()} STARTED".center(100, "-"))

    # for config file provided use the content of the JSON to create
    # the python statements needed to be inserted into the template
    logger.info(f"building dag - {config['name']}")

    dag_string = create_dag_string(
        logger,
        config.get("name"),
        {
            "description": config.get("description"),
            "tags": config.get("properties", {}).get("tags"),
        },
    )
    default_args = create_dag_args(logger, config.get("properties", {}).get("args"))
    imports = "\n".join(config.get("properties", {}).get("imports"))
    tasks = []
    dependencies = []

    # for each item in the task array, check the operator type and use this
    # to determine the task parameters to be used
    for t in config["tasks"]:
        task = Task(
            t.get("task_id"),
            t.get("operator"),
            t.get("parameters"),
            t.get("dependencies"),
        )
        logger.info(f'creating task "{task.task_id}" - {pop_stack()}')
        if task.operator == TaskOperator.CREATETABLE.name:
            # for each task, add a new one to config["tasks"] with data check tasks.
            if (
                "block_data_check" not in task.parameters.keys()
                or not task.parameters["block_data_check"]
            ):
                data_check_tasks = create_data_check_tasks(
                    logger, task, config["properties"]
                )
                for d in data_check_tasks:
                    if not d in config["tasks"]:
                        config["tasks"].append(d)

            task.parameters = create_table_task(logger, task, config["properties"])
            task.operator = TaskOperator.BQOPERATOR.value

        elif task.operator == "TruncateTable":
            task.parameters = create_table_task(logger, task, config["properties"])
            task.operator = TaskOperator.BQOPERATOR.value

        elif task.operator == "DataCheck":
            task.operator = TaskOperator.BQCHEK.value

        elif task.operator == TaskOperator.LOADFROMGCS.value:
            task.parameters = create_gcs_load_task(logger, task, config["properties"])
            task.operator = TaskOperator.GCSTOBQ.value

        tasks.append(create_task(logger, task))

        if len(task.dependencies) > 0:
            # for each entry in the dependencies array, add the item as a dependency.
            # where the dependency is on an external task, create an external task if
            # no task already exists
            for dep in task.dependencies:
                dep_list = dep.split(".")
                if len(dep_list) > 1:
                    dep_task = f"ext_{dep_list[1]}"
                    if not dep_task in [t.split(" ")[0].strip() for t in tasks]:
                        ext_task = Task(
                            f"{dep_task}",
                            "ExternalTaskSensor",
                            {
                                "external_dag_id": dep_list[0],
                                "external_task_id": dep_list[1],
                                "check_existence": True,
                                "timeout": 600,
                                "allowed_states": ["success"],
                                "failed_states": ["failed", "skipped"],
                                "mode": "reschedule",
                            },
                        )
                        tasks.append(create_task(logger, ext_task))
                        dependencies.append(f"start_pipeline >> {dep_task}")
                else:
                    dep_task = dep
                dependencies.append(f"{dep_task} >> {task.task_id}")
        else:
            dependencies.append(f"start_pipeline >> {task.task_id}")

    dep_tasks = [d[0].strip() for d in [dep.split(">") for dep in dependencies]]
    final_tasks = [
        task.get("task_id")
        for task in config["tasks"]
        if not task.get("task_id") in dep_tasks
    ]

    for task in final_tasks:
        dependencies.append(f"{task} >> finish_pipeline")

    properties = [
        f"{key} = '{config['properties'][key]}'" for key in config["properties"].keys()
    ]

    logger.info(f"populating template")
    file_loader = FileSystemLoader("./templates")
    env = Environment(loader=file_loader)

    template = env.get_template("template_dag.txt")
    output = template.render(
        imports=imports,
        tasks=tasks,
        default_args=default_args,
        dag_string=dag_string.replace("'", '"'),
        dependencies=dependencies,
        properties=properties,
    )

    # reformat dag files to pass linting
    reformatted = black.format_file_contents(output, fast=False, mode=black.FileMode())

    dag_file = f"{output_directory}{config['name']}.py"
    with open(dag_file, "w") as outfile:
        outfile.write(reformatted)

    logger.info(f"dag files {pop_stack()} COMPLETED SUCCESSFULLY".center(100, "-"))
    return 0


def create_data_check_tasks(logger: ILogger, task: Task, properties: dict) -> list:
    """
    This function creates a list of data check tasks for a given task

    Args:
      logger (ILogger): ILogger - this is the logger object that is passed to the function.
      task (Task): The task object that is being created.
      properties (dict): a dictionary of properties that are used to create the DAG.

    Returns:
      A list of data check tasks.
    """
    logger.info(f"{pop_stack()} STARTED".center(100, "-"))
    data_check_tasks = []

    if "source_to_target" in task.parameters.keys():
        table_keys = [
            field["name"]
            for field in task.parameters["source_to_target"]
            if "pk" in field.keys()
        ]

        history_keys = [
            field["name"]
            for field in task.parameters["source_to_target"]
            if "hk" in field.keys()
        ]
    else:
        table_keys = []
        history_keys = []

    logger.info(f"creating row count check")
    dataset = (
        task.parameters["destination_dataset"]
        if "destination_dataset" in task.parameters.keys()
        else properties["dataset_publish"]
    )
    table = task.parameters["destination_table"]
    row_count_check_task = SQLDataCheckTask(
        f"{task.parameters['destination_table']}_data_check_row_count",
        TaskOperator.DATACHECK,
        SQLDataCheckParameter(f"select count(*) from {dataset}.{table}"),
        [f"{task.task_id}"],
    )
    data_check_tasks.append(todict(row_count_check_task))

    # create task to check for duplicates on primary key - if primary key
    # fields specified in config.
    if len(table_keys) > 0:
        logger.info(f"creating duplicate data check")
        dupe_check_task = SQLDataCheckTask(
            f"{task.parameters['destination_table']}_data_check_duplicate_records",
            TaskOperator.DATACHECK,
            SQLDataCheckParameter(
                "sql/data_check_duplicate_records.sql",
                params={
                    "DATASET_ID": f"{task.parameters['destination_dataset']}"
                    if "destination_dataset" in task.parameters.keys()
                    else f"{properties['dataset_publish']}",
                    "FROM": f"{task.parameters['destination_table']}",
                    "KEY": f"{', '.join(table_keys)}",
                },
            ),
            [task.task_id],
        )
        data_check_tasks.append(todict(dupe_check_task))

    # create task to check for multiple open records - if primary key
    # fields specified in config.
    if len(table_keys) > 0 and task.parameters["target_type"] == TableType.HISTORY.name:
        logger.info(f"creating duplicate active history data check")
        dates = [
            "effective_from_dt",
            "effective_from_dt_csn_seq",
            "effective_from_dt_seq",
            "effective_to_dt",
        ]
        dupe_check_task = SQLDataCheckTask(
            f"{task.parameters['destination_table']}_data_check_open_history_items",
            TaskOperator.DATACHECK,
            SQLDataCheckParameter(
                "sql/data_check_open_history_items.sql",
                params={
                    "DATASET_ID": f"{task.parameters['destination_dataset']}"
                    if "destination_dataset" in task.parameters.keys()
                    else f"{properties['dataset_publish']}",
                    "FROM": f"{task.parameters['destination_table']}",
                    "KEY": f"{', '.join(history_keys)}",
                },
            ),
            [task.task_id],
        )
        data_check_tasks.append(todict(dupe_check_task))

    logger.info(f"dag files {pop_stack()} COMPLETED SUCCESSFULLY".center(100, "-"))
    return data_check_tasks


def create_gcs_load_task(logger: ILogger, task: Task, properties: dict) -> dict:
    """
    This function creates a task that loads data from a Google Cloud Storage bucket into a BigQuery
    table

    Args:
      logger (ILogger): ILogger - this is the logger object that is passed to the function.
      task (Task): the task object from the task list
      properties (dict): dict

    Returns:
      A dictionary with the following keys:
        bucket
        destination_dataset_table
        write_disposition
        create_disposition
        source_objects
        source_format
        field_delimiter
        skip_leading_rows
        schema_object
    """
    logger.info(f"{pop_stack()} STARTED".center(100, "-"))
    gs_source_bucket = (
        "{gs_source_bucket}"
        if not "bucket" in task.parameters.keys()
        else task.parameters["bucket"]
    )  # set to use variable from target file if not in task parameters

    dataset_source = (
        "{dataset_source}"
        if not "destination_dataset" in task.parameters.keys()
        else task.parameters["destination_dataset"]
    )

    destination_dataset_table = (
        f"{dataset_source}.{task.parameters['destination_table']}"
    )

    write_disposition = (
        "WRITE_APPEND"
        if not "write_disposition" in task.parameters.keys()
        else f"{task.parameters['write_disposition']}"
    )

    if "source_format" in task.parameters.keys():
        source_format = f"{task.parameters['source_format']}"
    else:
        obj = task.parameters["source_objects"][0]
        obj_ext = pathlib.Path(os.path.normpath(obj)).suffix

        if obj_ext == ".json":
            source_format = "NEWLINE_DELIMITED_JSON"
        else:
            source_format = obj_ext.replace(".", "").upper()

    obj = os.path.basename(task.parameters["schema_object"])
    schema_object = f"schema/{obj}"
    schema_source = os.path.normpath(task.parameters["schema_object"])
    schema_target = os.path.normpath(f"dags/{schema_object}")
    if not os.path.isfile(schema_source):
        logger.debug(f"      task id: {task.task_id}")
        logger.debug(f'schema object: {task.parameters["schema_object"]}')
        raise FileNotFoundError(f"'{schema_source}' not found.")
    # if schema file doesn't exist in dags/schema/ dir then copy it
    if not os.path.isfile(schema_target) and os.path.isfile(schema_source):
        logger.debug(f"Copying {schema_source} to {schema_target}")
        copy(
            schema_source,
            schema_target,
        )

    field_delimiter = (
        ","
        if not "field_delimiter" in task.parameters.keys()
        else f"{task.parameters['field_delimiter']}"
    )

    skip_leading_rows = (
        1
        if not "skip_leading_rows" in task.parameters.keys()
        else f"{task.parameters['skip_leading_rows']}"
    )

    outp = {
        "bucket": gs_source_bucket,
        "destination_dataset_table": destination_dataset_table,
        "write_disposition": write_disposition,
        "create_disposition": "CREATE_IF_NEEDED",
        "source_objects": task.parameters["source_objects"],
        "source_format": source_format,
        "field_delimiter": field_delimiter,
        "skip_leading_rows": skip_leading_rows,
        "schema_object": schema_object,
    }

    logger.info(f"{pop_stack()} COMPLETED SUCCESSFULLY".center(100, "-"))
    return outp


def create_table_task(logger: ILogger, task: Task, properties: dict) -> dict:
    """
    This function creates a table in the publish dataset using the sql file created in the previous step

    Args:
      logger (ILogger): ILogger - this is the logger object that is passed to the function.
      task (Task): the task object from the task file
      properties (dict): a dictionary of properties that are used in the pipeline.

    Returns:
      A dictionary with the following keys:
        - sql
        - destination_dataset_table
        - write_disposition
        - create_disposition
        - allow_large_results
        - use_legacy_sql
        - params
    """

    logger.info(f"{pop_stack()} STARTED".center(100, "-"))
    dataset_staging = properties["dataset_staging"]
    dataset_publish = (
        "{dataset_publish}"
        if not "destination_dataset" in task.parameters.keys()
        else task.parameters["destination_dataset"]
    )  # set to use variable from target file if not in task parameters
    destination_dataset_table = (
        f"{dataset_publish}.{task.parameters['destination_table']}"
    )

    # if user has provided a link to a .sql file, use it otherwise look to create .sql from source to target parameter
    if "sql" in task.parameters.keys():
        sql = task.parameters["sql"]
    else:
        task.parameters["source_to_target"] = [
            field
            for field in task.parameters["source_to_target"]
            if not field["name"] in ["dw_created_dt", "dw_last_modified_dt"]
        ]
        file_path = create_sql_file(logger, task, dataset_staging=dataset_staging)
        sql = f"{file_path.replace('./','')}"

    write_disposition = (
        "WRITE_TRUNCATE"
        if not "write_disposition" in task.parameters.keys()
        else f"{task.parameters['write_disposition']}"
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

    logger.info(f"{pop_stack()} COMPLETED SUCCESSFULLY".center(100, "-"))
    return outp


def create_task(logger: ILogger, task: Task) -> str:
    """
    > The function takes a task object and returns a string that can be used to create a task in Airflow

    Args:
      logger (ILogger): ILogger - the logger object
      task (Task): the task object

    Returns:
      A string that can be used to create a task in Airflow
    """
    logger.info(f"{pop_stack()} STARTED".center(100, "-"))
    logger.debug(
        f"""creating task {task.task_id} from:
                               parameters - {task.parameters}"""
    )

    outp = [f"{task.task_id} = {task.operator} (task_id='{task.task_id}'"]

    # for each key:value pair in the task parameters we perform checks based on
    # parameter type and create a value that can be appended to the string
    for key in task.parameters.keys():
        if type(task.parameters[key]) == int or type(task.parameters[key]) == bool:
            value = task.parameters[key]
        elif type(task.parameters[key]) == str:
            value = f"f'''{task.parameters[key]}'''"
        elif key == "params":
            value = {}
            params = task.parameters.get("params")
            if params:
                for p in params.keys():
                    value[p] = (
                        "f'{dataset_publish}'"
                        if task.parameters[key][p] == "{dataset_publish}"
                        else f"{params[p]}"
                    )

        else:
            value = f"{task.parameters[key]}"

        outp.append(f"{key} = {value}")
    outp.append("dag=dag)")

    logger.info(f"{pop_stack()} COMPLETED SUCCESSFULLY".center(100, "-"))
    return ",\n          ".join(outp)


def create_dag_string(logger: ILogger, name: str, dag: dict) -> str:
    """
    > This function takes a dictionary of DAG parameters and returns a string that can be used to create
    a DAG object in Airflow

    Args:
      logger (ILogger): ILogger - this is the logger object that is passed to the function
      name (str): The name of the DAG.
      dag (dict): properties of the DAG

    Returns:
      A string that is the DAG definition
    """
    logger.info(f"{pop_stack()} STARTED".center(100, "-"))
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

    logger.info(f"{pop_stack()} COMPLETED SUCCESSFULLY".center(100, "-"))
    return outp


def create_dag_args(logger: ILogger, args: dict) -> str:
    """
    It takes a dictionary of arguments and returns a string that can be used to create a DAG object in
    Airflow

    Args:
      logger (ILogger): ILogger - the logger object
      args (dict): dict = {

    Returns:
      A string that is a dictionary of the arguments for the DAG.
    """
    logger.info(f"{pop_stack()} STARTED".center(100, "-"))
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

    logger.info(f"{pop_stack()} COMPLETED SUCCESSFULLY".center(100, "-"))
    return outp
