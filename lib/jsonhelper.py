import fastjsonschema
import json
import os
import sys
import traceback

from lib.logger import ILogger, pop_stack

__all__ = ['IJSONValidate', 'get_config']

def IJSONValidate(logger: ILogger, schema: dict, object: dict) -> bool:
    """
    > This function validates a JSON object against a JSON schema
    
    Args:
      logger (ILogger): ILogger - this is the logger object that you can use to log messages.
      schema (dict): The schema to validate against.
      object (dict): The object to be validated
    """
    logger.info(f"{pop_stack()} - STARTED".center(100, "-"))
    validate = fastjsonschema.compile(schema)
    try:
        logger.info(f"{pop_stack()} - validating schema...")
        validate(object)
    except fastjsonschema.JsonSchemaValueException as e:
        logger.debug(f"{sys.exc_info()[1]:}")
        logger.info(f"{pop_stack()} - Schema not matching")
        logger.info(f"{pop_stack()} - COMPLETED SUCCESSFULLY".center(100, "-"))
        return False        
    logger.info(f"{pop_stack()} - Schema matching")
    logger.info(f"{pop_stack()} - COMPLETED SUCCESSFULLY".center(100, "-"))
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

    logger.info(f"{pop_stack()} - STARTED".center(100, "-"))
    if not path:
        logger.warning(f"{pop_stack()} - File {path:} does not exist.")
        return {}

    try:
        # identify what path is; dir, file
        if os.path.isdir(path) or not os.path.exists(path):
            raise FileExistsError
    except (FileNotFoundError, FileExistsError) as e:
        logger.error(f"{pop_stack()} - File {path:} does not exist.")
        logger.info(f"{pop_stack()} - FAILED")
        return
    except:
        logger.error(f"{sys.exc_info()[0]:}")
        logger.info(f"{pop_stack()} - FAILED")
        return

    # read file
    try:
        with open(path, "r") as sourcefile:
            filecontent = sourcefile.read()

        # return file
        logger.info(f"{pop_stack()} - COMPLETED SUCCESSFULLY".center(100, "-"))
        return json.loads(filecontent)
    except:
        logger.error(f"{pop_stack()} - {sys.exc_info()[0]:}")
        logger.info(f"{pop_stack()} - FAILED")
        return