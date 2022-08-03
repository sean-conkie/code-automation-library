import argparse
import os
import re
import sys
import traceback

from datetime import datetime
from lib.buildartifacts import buildartifacts
from lib.buildbatch import buildbatch
from lib.builddags import builddags
from lib.helper import ifnull
from lib.jsonhelper import get_json
from lib.logger import ILogger, pop_stack


def main(logger: ILogger, args: dict) -> int:
    """
    The function creates a list of config files using the source directory (dpath), if the path provided
    is a file add id otherwise append each filename in directory

    Args:
      logger (ILogger): ILogger - this is the logger object that is passed to the function
      args (dict): a dictionary of the command line arguments

    Returns:
      The return value is the exit code of the program.
    """

    logger.info(f"job files - {pop_stack()} STARTED".center(100, "-"))

    dpath = args.get("config")
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
        path = config if os.path.exists(config) else os.path.join(dpath, config)
        cfg = get_json(logger, path)
        job_type = cfg.get("type")
        if job_type == "DAG":
            if builddags(logger, args, cfg) != 0:
                logger.error(f"an error occured processing {path}")
                sys.exit(1)
        elif job_type == "BATCH":
            if buildbatch(logger, args, cfg) != 0:
                logger.error(f"an error occured processing {path}")
                sys.exit(1)
        else:
            logger.error(f"No job type supplied in {path}")

        buildartifacts(logger, args, cfg)

    logger.info(f"job files {pop_stack()} COMPLETED SUCCESSFULLY".center(100, "-"))
    return 0


def create_parameters(path: str = None) -> dict:
    """
    > This function creates a dictionary of parameters for the application

    Args:
      path (str): str = None

    Returns:
      A dictionary of parameters
    """

    if path and not os.path.exists(path):
        raise FileNotFoundError

    cfg = get_json(logger, path) if path else {}

    log_default = ifnull(os.environ.get("SYS_LOG"), "./batch_application/logs/")
    config_default = "./bq_application/job/"
    dag_default = "./dags/"
    dag_sql_default = "./dags/sql/"
    batch_scr_default = ifnull(
        os.environ.get("SYS_SCR"), "./batch_application/scripts/scr/"
    )
    batch_sql_default = ifnull(
        os.environ.get("SYS_LOG"), "./batch_application/scripts/sql/"
    )
    table_def_file_default = "./bq_application/table/"
    table_cfg_default = "./bq_application/cfg/"
    project_id = os.environ.get("PROJECT_ID")

    parameters = {
        "log": os.path.normpath(cfg.get("log", log_default)),
        "config": os.path.normpath(cfg.get("config", config_default)),
        "dag": os.path.normpath(cfg.get("dag", dag_default)),
        "dag_sql": os.path.normpath(cfg.get("dag_sql", dag_sql_default)),
        "batch_scr": os.path.normpath(cfg.get("batch_scr", batch_scr_default)),
        "batch_sql": os.path.normpath(cfg.get("batch_sql", batch_sql_default)),
        "table_def_file": os.path.normpath(
            cfg.get("table_def_file", table_def_file_default)
        ),
        "table_cfg": os.path.normpath(cfg.get("table_cfg", table_cfg_default)),
        "project_id": cfg.get("logs", project_id),
        "debug_level": cfg.get("debug_level", "DEBUG"),
    }
    return parameters


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        required=False,
        dest="config_path",
        help="Specify the location of the config file which define script parameters",
    )

    known_args, args = parser.parse_known_args()
    parameters = create_parameters(known_args.config_path)

    log_file_name = os.path.join(
        parameters.get("log"),
        f'buildjobs_{datetime.now().strftime("%Y-%m-%dT%H%M%S")}.log',
    )

    logger = ILogger("buildjobs", log_file_name, parameters.get("debug_level"))

    try:
        main(logger, parameters)
    except:
        logger.error(f"{traceback.format_exc():}")
        logger.debug(f"{sys.exc_info()[1]:}")
        logger.info(f"job files - {pop_stack()} FAILED".center(100, "-"))
