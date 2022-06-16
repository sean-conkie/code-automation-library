_all_ = ["isnullorwhitespace"]


def isnullorwhitespace(string: str) -> bool:
    """
    If the input is None, or if the input is a string that is empty or contains only whitespace, return
    True. Otherwise, return False

    Args:
      string (str): str

    Returns:
      A boolean value.
    """
    if string is None:
        return True

    if not type(string) == str:
        raise ValueError(f"Input must be of type Str (supplied type {type(string)}).")

    if not string or not string.strip():
        return True

    return False


def isnullorempty(string: str) -> bool:
    """
    > If the string is null, return false. If the string is not a string, raise an error. If the string
    is empty or only whitespace, return true. Otherwise, return false

    Args:
      string (str): The string to check.

    Returns:
      A boolean value.
    """
    if string is None:
        return False

    if not type(string) == str:
        raise ValueError(f"Input must be of type Str (supplied type {type(string)}).")

    if not string or not string.strip():
        return True

    return False


def ifnull(string: str, default: str) -> str:
    """
    If the string is null or whitespace, return the default string, otherwise return the string

    Args:
      string (str): The string to check.
      default (str): The default value to return if the string is null or whitespace.

    Returns:
      The string is being returned if it is not null or whitespace.
    """
    if isnullorwhitespace(string):
        return default
    else:
        return string
