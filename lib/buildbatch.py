import os
import re

from lib.baseclasses import (
    TableType,
    TaskOperator,
    Task,
    SQLDataCheckTask,
    SQLDataCheckParameter,
    todict,
)
from datetime import datetime
from jinja2 import Environment, FileSystemLoader
from lib.helper import FileType, format_description
from lib.logger import format_message, ILogger
from lib.sql_helper import create_sql_file

__all__ = [
    "buildbatch",
]


# following variables are set global to allow recursive
# transformations to be applied
DEPENDENCIES = []
SUB_PROCESS_DICT = {}


def buildbatch(logger: ILogger, args: dict, config: dict) -> int:
    """
    The function takes a logger, a dictionary of arguments and a dictionary of configuration and returns
    an integer

    Args:
      logger (ILogger): ILogger - this is the logger object that is used to log messages to the console
    and to the log file.
      args (dict): the command line arguments
      config (dict): the JSON file that contains the configuration for the batch file

    Returns:
      0
    """

    logger.info(f"batch files - STARTED".center(100, "-"))

    # for config file provided use the content of the JSON to create
    # the statements needed to be inserted into the template
    logger.info(format_message(f"building process - {config['name']}"))

    tasks = []
    scripts = []

    # for each item in the task array, check the operator type and use this
    # to determine the task parameters to be used
    for i, t in enumerate(config["tasks"]):
        task = Task(
            t.get("task_id"),
            t.get("operator"),
            t.get("parameters"),
            t.get("author"),
            t.get("dependencies"),
            t.get("description"),
        )
        logger.info(f'creating task "{task.task_id}"')
        if task.operator == TaskOperator.CREATETABLE.name:
            # for each task, add a new one to config["tasks"] with data check tasks.
            if not task.parameters.get("block_data_check"):
                data_check_tasks = create_data_check_tasks(
                    logger, task, config["properties"]
                )
                for d in data_check_tasks:
                    if not d in config["tasks"]:
                        config["tasks"].append(d)

            sub_process = create_table_task(
                logger, task, config.get("properties", {}), args, config["name"]
            )

        SUB_PROCESS_DICT[sub_process] = i
        tasks.append(task.task_id)
        scripts.append(f"{task.task_id}.sql")

        for d in task.dependencies:
            d_sub_process = d.replace(
                config.get("properties", {}).get("prefix", "") + "_", ""
            ).upper()
            d_file = d.replace(config.get("properties", {}).get("prefix", "") + "_", "")

            DEPENDENCIES.extend([(sub_process, f"'{d_sub_process}|{d_file}|Y '\\")])

    if len(DEPENDENCIES) > 0:
        logger.info(f"calculating dependencies")

        for dep in DEPENDENCIES:
            dependency_re_order(logger, dep)

    else:
        logger.info(f"no dependencies")

    sub_process_list = list(
        dict(sorted(SUB_PROCESS_DICT.items(), key=lambda item: item[1])).keys()
    )

    logger.info(f"creating template parameters")

    logger.info(f"populating templates")
    file_loader = FileSystemLoader("./templates")
    env = Environment(loader=file_loader)

    scr_template = env.get_template("template_scr.txt")
    scr_output = scr_template.render(
        job_id=config.get("name", "").lower(),
        created_date=datetime.now().strftime("%d %b %Y"),
        tasks=format_description(" ".join(tasks), "", FileType.SH),
        description=format_description(task.description, "Description", FileType.SH),
        scripts=format_description(" ".join(scripts), "", FileType.SH),
        cut=len(config.get("properties", {}).get("prefix") + "_"),
        sub_process_list=re.sub(
            r"(\\$(?!\n))",
            "",
            "\n".join(sub_process_list),
            re.IGNORECASE,
        ),
        author=task.author,
    )

    scr_file = os.path.join(args.get("batch_scr"), f"{config['name']}.sh")
    with open(scr_file, "w") as outfile:
        outfile.write(scr_output)

    logger.info(format_message(f"Job file created: {config['name']}.sh"))

    pct_template = env.get_template("template_pct.txt")
    pct_output = pct_template.render(
        job_id=config.get("name", "").lower(),
        created_date=datetime.now().strftime("%d %b %Y"),
        author=task.author,
    )

    pct_file = os.path.join(args.get("batch_scr"), f"pct_{config['name']}.sh")
    with open(pct_file, "w") as outfile:
        outfile.write(pct_output)

    logger.info(format_message(f"Pop control file created: pct_{config['name']}.sh"))

    logger.info(f"batch files COMPLETED SUCCESSFULLY".center(100, "-"))
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
    logger.info(f"STARTED".center(100, "-"))
    data_check_tasks = []

    table_keys = [
        field["name"]
        for field in task.parameters.get("source_to_target", [])
        if "pk" in field.keys()
    ]

    history_keys = [
        field["name"]
        for field in task.parameters.get("source_to_target", [])
        if "hk" in field.keys()
    ]

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

    logger.info(f"dag files COMPLETED SUCCESSFULLY".center(100, "-"))
    return data_check_tasks


def create_table_task(
    logger: ILogger, task: Task, properties: dict, args: dict, job_name: str
) -> dict:
    """
    It creates a SQL file for the task, and returns a string that will be used to create a SQL file for
    the batch

    Args:
      logger (ILogger): ILogger - this is the logger object that is passed to the function.
      task (Task): The task object that is being processed.
      properties (dict): This is a dictionary of all the properties that are defined in the properties
    file.
      args (dict): This is the dictionary of arguments passed to the script.

    Returns:
      The task_id is being returned.
    """

    logger.info(f"STARTED".center(100, "-"))
    dataset_staging = properties.get("dataset_staging")

    if not task.parameters.get("sql"):
        task.parameters["source_to_target"] = [
            field
            for field in task.parameters.get("source_to_target")
            if not field.get("name") in ["dw_created_dt", "dw_last_modified_dt"]
        ]
        create_sql_file(
            logger,
            task,
            file_path=args.get("batch_sql"),
            dataset_staging=dataset_staging,
            job_id=job_name,
        )

    outp = f"'{task.task_id.replace(properties.get('prefix','') + '_', '').upper()}|{task.task_id.replace(properties.get('prefix','') + '_', '')}|Y '\\"

    logger.info(f"COMPLETED SUCCESSFULLY".center(100, "-"))
    return outp


def dependency_re_order(logger: ILogger, dependant_pair: tuple):
    logger.info(f"STARTED".center(100, "-"))
    if SUB_PROCESS_DICT[dependant_pair[0]] < SUB_PROCESS_DICT[dependant_pair[1]]:
        logger.info(f"Ordering pair {dependant_pair}")
        SUB_PROCESS_DICT[dependant_pair[0]] = SUB_PROCESS_DICT[dependant_pair[1]] + 1

        for dep in DEPENDENCIES:
            if dep[1] == dependant_pair[0]:
                logger.info(f"Re-calculating impacted dependencies")
                dependency_re_order(logger, dep)

    logger.info(f"batch files COMPLETED SUCCESSFULLY".center(100, "-"))
    return 0
