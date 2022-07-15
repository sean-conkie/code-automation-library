import re

from enum import Enum

__all__ = [
    "isnullorwhitespace",
    "isnullorempty",
    "ifnull",
    "FileType",
    "format_description",
]


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


class FileType(Enum):
    SH = 1
    SQL = 2


def format_description(description: str, section: str, target_type: FileType) -> str:
    """
    It takes a string, and returns a string

    Args:
      description (str): The description of the function
      section (str): The section of the script that the description is for.
      target_type (FileType): The type of file you want to generate.
    """
    if target_type == FileType.SH:
        prefix = "#"
        justify = 16
    elif target_type == FileType.SQL:
        prefix = "--"
        justify = 17

    if description:
        first_line_prefix = f"{prefix} {section}"
        first_line_prefix = (
            f"{first_line_prefix} {':'.rjust(justify - len(first_line_prefix))}"
        )

        line_prefix = f"{prefix}"
        line_prefix = f"{line_prefix} {':'.rjust(justify - len(line_prefix))}"

        pattern = r"(\b[\w\.]+\b)"
        m = re.findall(pattern, description, re.IGNORECASE)

        lines = []
        line = [first_line_prefix]
        line_length = 80
        line_length_cnt = len(first_line_prefix)
        for word in m:
            if (line_length_cnt + len(word) + len(line)) < line_length:
                line.append(word)
                line_length_cnt += len(word)
            else:
                lines.append(line)
                line = [line_prefix, word.strip()]
                line_length_cnt = len(word) + 20

        if not line in lines:
            lines.append(line)

        joined_lines = []
        for line in lines:
            joined_lines.append(" ".join(line))

        return "\n".join(joined_lines)

    return prefix
