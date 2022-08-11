import argparse
import os
import re
import sys
import traceback

from datetime import datetime
from lib.jsonhelper import IJSONValidate, get_json
from lib.logger import format_message, ILogger


def main(logger: ILogger, args: argparse.Namespace):
    """
    This function validates the config file(s) against a schema

    Args:
      logger (ILogger): ILogger - this is the logger object that is passed to the function.
      args (argparse.Namespace): argparse.Namespace

    Returns:
      The exit code of the program.
    """

    logger.info(f"Config Validate STARTED".center(100, "-"))
    if args.config_directory:
        # create a list of config files using the source directory (args.config_directory)
        dpath = os.path.normpath(args.config_directory)
        config_list = []
        try:
            logger.info(f"creating config list")
            for filename in os.listdir(dpath):
                logger.debug(format_message(f"filename: {filename}"))
                m = re.search(r"^cfg_.*\.json$", filename, re.IGNORECASE)
                if m:
                    p = os.path.normpath(f"{dpath}/{filename}")
                    config_list.append(p)
        except:
            logger.error(f"{sys.exc_info()[0]:}")
            logger.info(f"Config Validate FAILED")
            return 1

    elif args.config_list:
        config_list = args.config_list.split(",")
    else:
        raise Exception("No file provided to validate.")

    exit_code = 0

    for c in config_list:
        cpath = c.strip()
        logger.info(format_message(f"validating file: {cpath}"))
        config = get_json(logger, cpath)
        if not config:
            return 1

        schema = get_json(logger, "./bq_application/job/schema_cfg_job.json")
        if not schema:
            return 1

        logger.info(f"validate schema object")
        result = IJSONValidate(logger, schema, config)
        if not result:
            exit_code = 1

        if result and "properties" in config.keys():
            logger.info(f"validate properties object")
            if config.get("type") == "BATCH":
                properties_schema = get_json(
                    logger, "./bq_application/job/schema_cfg_batch_properties.json"
                )

                if properties_schema:
                    logger.debug(f"validating properties: {config.get('type')}")
                    properties_check_result = IJSONValidate(
                        logger, properties_schema, config.get("properties", {})
                    )
                    if not properties_check_result:
                        exit_code = 1

                else:
                    logger.debug(
                        f"skipped properties validation ({config.get('type', '<missing job type>')})"
                    )

        if result and "tasks" in config.keys():
            logger.info(f"validate task object(s)")
            for t in config["tasks"]:
                task_schema = None
                if t["operator"] == "CREATETABLE":
                    task_schema = get_json(
                        logger, "./bq_application/job/schema_cfg_createtable_task.json"
                    )

                if task_schema:
                    logger.debug(f"validating task: {t['task_id']}")
                    task_check_result = IJSONValidate(logger, task_schema, t)
                    if not task_check_result:
                        exit_code = 1
                else:
                    logger.debug(
                        format_message(
                            f"skipped task: {t['task_id'] if 'task_id' in t.keys() else '<missing task id>'} ({t['operator'] if 'operator' in t.keys() else '<missing task operator>'})"
                        )
                    )
        else:
            logger.info(f"task validation skipped")
            skip_reason = (
                "No tasks to validate."
                if result
                else f"Object schema validation failed"
            )
            logger.debug(f"task validation skipped: {skip_reason}")

    if exit_code != 0:
        logger.error(
            f"One or more files have failed validation, check logs for more information."
        )

    logger.info(f"Config Validate COMPLETED SUCCESSFULLY".center(100, "-"))
    return exit_code


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config_directory",
        required=False,
        default="./bq_application/job/",
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
    parser.add_argument(
        "--log_directory",
        required=False,
        dest="log_dir",
        default=None,
        help="Specify the desired output directory for logs.  No dir means no log file will be output.",
    )

    known_args, args = parser.parse_known_args()

    log_file_name = (
        os.path.normpath(
            f'{known_args.log_dir}/validatedagconfig_{datetime.now().strftime("%Y-%m-%dT%H%M%S")}.log'
        )
        if known_args.log_dir
        else None
    )
    logger = ILogger("Config Validate", log_file_name, level=known_args.level)

    try:
        result = main(logger, known_args)
    except:
        logger.error(f"{traceback.format_exc():}")
        logger.debug(f"{sys.exc_info()[1]:}")
        logger.info(f"Config Validate FAILED".center(100, "-"))
        result = 1

    if result != 0:
        raise Exception("Exiting with errors found!")
