import fastjsonschema
import json
import os
import sys

from lib.logger import format_message, ILogger

__all__ = ["IJSONValidate", "get_config"]


def IJSONValidate(logger: ILogger, schema: dict, object: dict) -> bool:
    """
    > This function validates a JSON object against a JSON schema

    Args:
      logger (ILogger): ILogger - this is the logger object that you can use to log messages.
      schema (dict): The schema to validate against.
      object (dict): The object to be validated
    """
    logger.info(f"STARTED".center(100, "-"))
    validate = fastjsonschema.compile(schema)
    try:
        logger.info(f"validating schema...")
        validate(object)
    except fastjsonschema.JsonSchemaValueException as e:
        logger.debug(f"{sys.exc_info()[1]:}")
        logger.error(f"Schema not matching")
        logger.info(f"COMPLETED SUCCESSFULLY".center(100, "-"))
        return False
    logger.info(f"Schema matching")
    logger.info(f"COMPLETED SUCCESSFULLY".center(100, "-"))
    return True


def get_json(logger: ILogger, path: str) -> dict:
    """
    This function takes a path to a json file and returns a
    dictionary object

    Args:
      path: The path to the json file you want to read.

    Returns:
      A dictionary object
    """

    logger.info(f"STARTED".center(100, "-"))
    if not path:
        logger.warning(format_message(f"File {path:} does not exist."))
        return {}

    logger.debug(format_message(f"File input {path:}."))

    try:
        # identify what path is; dir, file
        if os.path.isdir(os.path.normpath(path)) or not os.path.exists(
            os.path.normpath(path)
        ):
            raise FileExistsError
    except (FileNotFoundError, FileExistsError) as e:
        logger.error(format_message(f"File {path:} does not exist."))
        logger.info(f"FAILED".center(100, "-"))
        return None
    except:
        logger.error(f"{sys.exc_info()[0]:}")
        logger.info(f"FAILED".center(100, "-"))
        return None

    # read file
    try:
        with open(path, "r") as sourcefile:
            filecontent = sourcefile.read()

        # return file
        logger.info(f"COMPLETED SUCCESSFULLY".center(100, "-"))
        return json.loads(filecontent)
    except:
        logger.error(f"{sys.exc_info()[0]:}")
        logger.info(f"FAILED".center(100, "-"))
        return None
