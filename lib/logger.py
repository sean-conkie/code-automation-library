import re

__all__ = [
    "ILogger",
    "format_message",
]

from logging import Logger, NOTSET, Formatter, FileHandler, StreamHandler

# It creates a logger object with the given name and level
class ILogger(Logger):
    def __init__(self, name: str, file: str = None, level=NOTSET):
        """
        This function sets the logger object.  It takes in a logger name and a logger level and returns an integer

        Args:
          logger_name (str): The name of the logger.
          logger_level (str): The level of logging you want to see. This can be one of the following:'CRITICAL',
          'DEBUG', 'ERROR', 'FATAL','INFO','NOTSET', 'WARNING'
        Defaults to INFO

        Returns:
          The return value is the exit code of the function.
        """
        Logger.__init__(self, name, level)
        formatter = Formatter(
            "%(asctime)s - %(name)s - [%(levelname)8s] - %(funcName)20s:%(lineno)5d - %(message)s"
        )

        streamHandler = StreamHandler()
        streamHandler.setFormatter(formatter)

        if file:
            fileHandler = FileHandler(file, mode="a")
            fileHandler.setFormatter(formatter)
            self.addHandler(fileHandler)

        self.addHandler(streamHandler)


def format_message(message: str) -> str:
    if message:

        m = re.findall(r"([^\s]+)", message, re.IGNORECASE)

        prefix = "".ljust(79)
        lines = []
        line = []
        line_length = 180
        line_length_cnt = len(prefix)
        for word in m:
            if (line_length_cnt + len(word) + len(line)) < line_length:
                line.append(word)
                line_length_cnt += len(word)
            else:
                lines.append(line)
                line = [prefix, word.strip()]
                line_length_cnt = len(word) + 20

        if not line in lines:
            lines.append(line)

        joined_lines = []
        for line in lines:
            joined_lines.append(" ".join(line))

        return "\n".join(joined_lines)

    return ""
