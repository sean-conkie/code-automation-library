import json
import re

from lib.baseclasses import Task, converttoobj, ConversionType
from lib.logger import ILogger, pop_stack

__all__ = [
    "buildartifacts",
]


def buildartifacts(logger: ILogger, output_directory: str, config: dict) -> int:

    logger.info(f"buildartifacts - {pop_stack()} STARTED".center(100, "-"))

    # for config file provided use the content of the JSON to create
    # the statements needed to be inserted into the template
    logger.info(f"creating object artifacts - {config['name']}")

    tasks = []

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

        t["parameters"]["source_to_target"] = converttoobj(
            t["paraneters"].get("source_to_target", []), ConversionType.SOURCE
        )

        t["parameters"]["joins"] = converttoobj(
            t["paraneters"].get("joins", []), ConversionType.JOIN
        )
        t["parameters"]["where"] = converttoobj(
            t["paraneters"].get("where", []), ConversionType.WHERE
        )

        if t.parameters.get("build_artifacts"):
            logger.info(f'creating artifacts for "{task.task_id}" - {pop_stack()}')
            table_def_content = [
                {
                    "name": field.name,
                    "type": field.data_type,
                    "mode": "nullable" if field.nullable else "required",
                }
                for field in t["parameters"]["source_to_target"]
            ]
            table_definition = task["parameters"].get(
                "destination_table", "table_definition"
            )
            with open(f"{output_directory}{table_definition}.json", "w") as outfile:
                outfile.write(json.dumps(table_def_content))

            logger.info(
                f'table definition created "{table_definition}.json" - {pop_stack()}'
            )

            tables = [
                field.source_name
                for field in t["parameters"]["source_to_target"]
                if field.source_name
                and re.search(r"_tds_", field.source_name, re.IGNORECASE)
            ]

            pattern = r"(\w+_tds_\w+\.\w+)\."
            for join in t["parameters"]["joins"]:
                for condition in join.on:
                    tables.extend(
                        re.findall(pattern, " ".join(condition.fields), re.IGNORECASE)
                    )

            for condition in t["parameters"]["where"]:
                tables.extend(
                    re.findall(pattern, " ".join(condition.fields), re.IGNORECASE)
                )

            tables = set(tables)

            table_build_config = [
                {
                    "object_name": table_definition,
                    "object_type": "type",
                    "dataset_name": t["parameters"].get(
                        "destination_dataset", config["properties"]["dataset_publish"]
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
                            {"project_id": config["properties"]["project_id"]},
                            {"data_set": table.split(".")[0]},
                            {"table_name": table.split(".")[1]},
                        ],
                    }
                    for table in tables
                ]
            )

            with open(f"{output_directory}cfg_{table_definition}.json", "w") as outfile:
                outfile.write(json.dumps(table_build_config))

            logger.info(
                f'table build config created "cfg_{table_definition}.json" - {pop_stack()}'
            )

    logger.info(f"batch files {pop_stack()} COMPLETED SUCCESSFULLY".center(100, "-"))
    return 0
