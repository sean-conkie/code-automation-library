import argparse
import os
import re
import sys
import traceback

from lib.buildbatch import buildbatch
from lib.builddags import builddags
from datetime import datetime
from lib.jsonhelper import get_json
from lib.logger import ILogger, pop_stack


def main(logger: ILogger, args: argparse.Namespace) -> int:

    logger.info(f"job files - {pop_stack()} STARTED".center(100, "-"))

    dpath = args.config_directory
    config_list = []

    # create a list of config files using the source directory (dpath), if the
    # path provided is a file add id otherwise append each filename in directory
    logger.info(f"creating config list")
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
        job_type = cfg.get("type")
        if job_type == "DAG":
            if builddags(logger, args.output_directory, cfg) != 0:
                logger.error(f"an error occured processing {path}")
                sys.exit(1)
        elif job_type == "BATCH":
            if buildbatch(logger, args.output_directory, cfg) != 0:
                logger.error(f"an error occured processing {path}")
                sys.exit(1)
        else:
            logger.error(f"No job type supplied in {path}")

    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config_directory",
        required=False,
        dest="config_directory",
        default="./cfg/job/",
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
        f'./logs/buildjobs_{datetime.now().strftime("%Y-%m-%dT%H%M%S")}.log'
    )
    logger = ILogger("buildjobs", log_file_name, known_args.level)

    try:
        main(logger, known_args)
    except:
        logger.error(f"{traceback.format_exc():}")
        logger.debug(f"{sys.exc_info()[1]:}")
        logger.info(f"job files - {pop_stack()} FAILED".center(100, "-"))
