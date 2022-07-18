import json
import os
import re

from lib.baseclasses import converttoobj, ConversionType, Field, Task
from lib.helper import ifnull
from lib.logger import ILogger, pop_stack

__all__ = [
    "buildartifacts",
]


def buildartifacts(logger: ILogger, args: dict, config: dict) -> int:
    """
    This function creates the table definition and table build config files for the objects defined in
    the config file

    Args:
      logger (ILogger): ILogger - this is the logger object that is used to log messages to the console
    and to the log file.
      args (dict): The command line arguments passed to the script.
      config (dict): The configuration file that was passed in.

    Returns:
      The return value is the exit code of the function.
    """
    logger.info(f"buildartifacts - {pop_stack()} STARTED".center(100, "-"))

    # for config file provided use the content of the JSON to create
    # the statements needed to be inserted into the template
    logger.info(f"creating object artifacts - {config['name']}")

    # for each item in the task array, check the operator type and use this
    # to determine the task parameters to be used
    for t in config["tasks"]:
        task = Task(
            t.get("task_id"),
            t.get("operator"),
            t.get("parameters", {}),
            t.get("author"),
            t.get("dependencies"),
            t.get("description"),
        )

        task.parameters["source_to_target"] = converttoobj(
            task.parameters.get("source_to_target", []), ConversionType.SOURCE
        )

        dw_index = 1
        for i, field in enumerate(task.parameters["source_to_target"]):
            if not field.pk:
                dw_index = i
                break
        task.parameters["source_to_target"].insert(
            dw_index,
            Field(
                name="dw_last_modified_dt",
            ),
        )

        if task.parameters.get("delta"):
            task.parameters["source_to_target"].insert(
                dw_index,
                Field(
                    name="dw_created_dt",
                ),
            )

        task.parameters["joins"] = converttoobj(
            task.parameters.get("joins", []), ConversionType.JOIN
        )
        task.parameters["where"] = converttoobj(
            task.parameters.get("where", []), ConversionType.WHERE
        )

        if task.parameters.get("build_artifacts", True):
            logger.info(f'creating artifacts for "{task.task_id}" - {pop_stack()}')
            table_def_content = [
                {
                    "name": field.name,
                    "type": field.data_type,
                    "mode": "nullable" if field.nullable else "required",
                }
                for field in task.parameters["source_to_target"]
            ]
            table_definition = task.parameters["destination_table"]
            with open(
                os.path.join(args.get("table_def_file"), f"{table_definition}.json"),
                "w",
            ) as outfile:
                outfile.write(json.dumps(table_def_content))

            logger.info(
                f'table definition created "{table_definition}.json" - {pop_stack()}'
            )

            tables = [
                field.source_name
                for field in task.parameters["source_to_target"]
                if field.source_name
                and re.search(r"_tds_", field.source_name, re.IGNORECASE)
            ]

            pattern = r"(\w+_tds_\w+\.\w+)\."
            if task.parameters["joins"]:
                for join in task.parameters["joins"]:
                    for condition in join.on:
                        tables.extend(
                            re.findall(
                                pattern, " ".join(condition.fields), re.IGNORECASE
                            )
                        )
            if task.parameters["where"]:
                for condition in task.parameters["where"]:
                    tables.extend(
                        re.findall(pattern, " ".join(condition.fields), re.IGNORECASE)
                    )

            tables = set(tables)

            table_build_config = [
                {
                    "object_name": table_definition,
                    "object_type": "type",
                    "dataset_name": ifnull(
                        task.parameters["destination_dataset"],
                        config["properties"]["dataset_publish"],
                    ),
                    "def_file": f"{table_definition}.json",
                }
            ]

            table_build_config.extend(
                [
                    {
                        "object_name": table.split(".")[1],
                        "object_type": "view",
                        "dataset_name": table.split(".")[0],
                        "def_file": "select_all_from_tab.sql",
                        "src_env_override": True,
                        "query_vars": [
                            {
                                "project_id": re.sub(
                                    r"(-\w+$)",
                                    "-ENV",
                                    task.parameters.get(
                                        "source_project_override", {}
                                    ).get(
                                        table, config["properties"]["source_project"]
                                    ),
                                    re.IGNORECASE,
                                )
                            },
                            {"data_set": table.split(".")[0]},
                            {"table_name": table.split(".")[1]},
                        ],
                    }
                    for table in tables
                ]
            )

            with open(
                os.path.join(args.get("table_cfg"), f"cfg_{table_definition}.json"), "w"
            ) as outfile:
                outfile.write(json.dumps(table_build_config))

            logger.info(
                f'table build config created "cfg_{table_definition}.json" - {pop_stack()}'
            )

    logger.info(f"buildartifacts {pop_stack()} COMPLETED SUCCESSFULLY".center(100, "-"))
    return 0
