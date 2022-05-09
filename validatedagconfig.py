import argparse
import json
import os
import re
import sys
import traceback

from datetime import datetime
from lib.jsonhelper import IJSONValidate, get_json
from lib.logger import ILogger, pop_stack


def main(logger: ILogger, args: argparse.Namespace):

    logger.info(f"validate config STARTED".center(100, "-"))
    if args.config_directory:
        # create a list of config files using the source directory (args.config_directory)
        config_list = []
        try:
            logger.info(f"{pop_stack()} - creating config list")
            for filename in os.listdir(args.config_directory):
                logger.debug(f"filename: {filename}")
                m = re.search(r"^cfg_.*\.json$", filename, re.IGNORECASE)
                if m:
                    config_list.append(filename)
        except:
            logger.error(f"{pop_stack()} - {sys.exc_info()[0]:}")
            logger.info(f"{pop_stack()} - dag files FAILED")
            return 1

    elif args.config_list:
        config_list = args.config_list.split(",")
    else:
        raise Exception("No file provided to validate.")

    for c in config_list:
        cpath = c.strip()
        logger.info(f"{pop_stack()} - validating file: {cpath}")
        config = get_json(logger, cpath)
        schema = get_json(logger, "cfg\dag\dag_cfg_schema.json")
        logger.info(f"{pop_stack()} - validate schema object")
        result = IJSONValidate(logger, schema, config)
        logger.info(f"{pop_stack()} - schema valid? {result}")

        if result and "tasks" in config.keys():
            logger.info(f"{pop_stack()} - validate task object(s)")
            for t in config["tasks"]:
                if t["operator"] == "CreateTable":
                    task_schema = get_json(
                        logger, "cfg\dag\dag_cfg_createtable_task_schema.json"
                    )
                    task_check_result = IJSONValidate(logger, schema, config)
        else:
            logger.info(f"{pop_stack()} - task validation skipped")
            skip_reason = (
                "No tasks to validate."
                if result
                else f"Object schema validation failed"
            )
            logger.debug(f"{pop_stack()} - task validation skipped: {skip_reason}")

    logger.info(f"validate config COMPLETED SUCCESSFULLY".center(100, "-"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config_directory",
        required=False,
        dest="config_directory",
        help="Specify the location of the config file(s) which are to be validated.",
    )
    parser.add_argument(
        "--config_list",
        required=False,
        dest="config_list",
        help="A list of paths to files to be validated.",
    )
    parser.add_argument(
        "--log_level",
        required=False,
        dest="level",
        default="DEBUG",
        help="Specify the desired log level (default: DEBUG).  This can be one of the following: 'CRITICAL', 'DEBUG', 'ERROR', 'FATAL','INFO','NOTSET', 'WARNING'",
    )

    known_args, args = parser.parse_known_args()

    logger = ILogger("JSON Validate", level=known_args.level)

    try:
        main(logger, known_args)
    except:
        logger.error(f"{traceback.format_exc():}")
        logger.debug(f"{sys.exc_info()[1]:}")
        logger.info(f"dag files FAILED".center(100, "-"))
