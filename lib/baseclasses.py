from enum import Enum
from unittest.util import strclass
from warnings import warn

__all__ = [
    "Condition",
    "Field",
    "Task",
    "LogicOperator",
    "Join",
    "Operator",
    "JoinType",
]


class LogicOperator(Enum):

    AND = "and"
    OR = "or"
    NOT = "not"
    NONE = None


class Operator(Enum):

    EQ = "="
    GT = ">"
    GE = ">="
    NE = "!="
    LG = "<>"
    LT = "<"
    LE = "<="
    NONE = None


class JoinType(Enum):
    LEFT = "left"
    INNER = "inner"
    FULL = "full"
    CROSS = "cross"


class WriteDisposition(Enum):
    WRITE_APPEND = "WRITE_APPEND"
    WRITE_TRANSIENT = "WRITE_TRANSIENT"
    WRITE_TRUNCATE = "WRITE_TRUNCATE"


class Condition(object):
    def __init__(
        self,
        fields: list[str],
        condition: LogicOperator = LogicOperator.NONE,
        operator: Operator = Operator.NONE,
    ) -> None:

        if len(fields) < 2:
            raise ValueError("Two fields must be provided for each condition.")
        elif len(fields) > 2:
            warn("Only the first 2 fields will be considered", SyntaxWarning, 1)

        self._fields = fields
        self._condition = condition
        self._operator = operator

    def __str__(self) -> str:
        return f"{self._condition.value} {self._fields[0]} {self._operator.value} {self._fields[1]}"

    @property
    def fields(self) -> str:
        """
        Returns the fields
        """
        return self._fields

    @fields.setter
    def fields(self, value: str) -> None:
        """
        Sets the fields
        """
        self._fields = value

    @property
    def condition(self) -> LogicOperator:
        """
        Returns the condition
        """
        return self._condition

    @condition.setter
    def condition(self, value: LogicOperator) -> None:
        """
        Sets the condition
        """
        self._condition = value

    @property
    def operator(self) -> Operator:
        """
        Returns the operator
        """
        return self._operator

    @operator.setter
    def operator(self, value: Operator) -> None:
        """
        Sets the operator
        """
        self._operator = value


class Join(object):
    def __init__(
        self,
        right: str,
        on: list[Condition],
        left: str = None,
        join_type: JoinType = JoinType.LEFT,
    ) -> None:

        self._left = left
        self._right = right
        self._on = on
        self._join_type = join_type

    @property
    def right(self) -> str:
        """
        Returns the right table
        """
        return self._right

    @right.setter
    def right(self, value: str) -> None:
        """
        Sets the right table
        """
        self._right = value

    @property
    def on(self) -> list[Condition]:
        """
        Returns the on conditions
        """
        return self._on

    @on.setter
    def on(self, value: list[Condition]) -> None:
        """
        Sets the on conditions
        """
        self._on = value

    @property
    def left(self) -> strclass:
        """
        Returns the left
        """
        return self._left

    @left.setter
    def left(self, value: str) -> None:
        """
        Sets the left
        """
        self._left = value

    @property
    def join_type(self) -> JoinType:
        """
        Returns the join_type
        """
        return self._join_type

    @join_type.setter
    def join_type(self, value: JoinType) -> None:
        """
        Sets the join_type
        """
        self._join_type = value


class Field(object):
    def __init__(
        self,
        name: str,
        source_column: str = None,
        source_name: str = None,
        transformation: str = None,
        pk: bool = None,
        hk: bool = None,
    ) -> None:
        if transformation:
            self._transformation = transformation
        elif source_column:
            self._source_column = source_column
            self._source_name = source_name
        else:
            raise ValueError(
                "Either 'transformation' or 'source_name' must be provided."
            )

        self._name = name
        self._pk = pk
        self._hk = hk

    @property
    def name(self) -> str:
        """
        Returns the name
        """
        return self._name

    @name.setter
    def name(self, value: str) -> None:
        """
        Sets the name
        """
        self._name = value

    @property
    def source_column(self) -> str:
        """
        Returns the source_column
        """
        return self._source_column

    @source_column.setter
    def name(self, value: str) -> None:
        """
        Sets the source_column
        """
        self._source_column = value

    @property
    def source_name(self) -> str:
        """
        Returns the source_name
        """
        return self._source_name

    @source_name.setter
    def source_name(self, value: str) -> None:
        """
        Sets the source_name
        """
        self._source_name = value

    @property
    def transformation(self) -> str:
        """
        Returns the transformation
        """
        return self._transformation

    @transformation.setter
    def transformation(self, value: str) -> None:
        """
        Sets the transformation
        """
        self._transformation = value

    @property
    def pk(self) -> bool:
        """
        Returns the pk
        """
        return self._pk

    @pk.setter
    def pk(self, value: bool) -> None:
        """
        Sets the pk
        """
        self._pk = value

    @property
    def hk(self) -> bool:
        """
        Returns the hk
        """
        return self._hk

    @hk.setter
    def hk(self, value: bool) -> None:
        """
        Sets the hk
        """
        self._hk = value


class Task(object):
    def __init__(
        self,
        task_id: str,
        operator: str,
        parameters: dict,
        dependencies: list[str] = [],
    ) -> None:
        self._task_id = task_id
        self._operator = operator
        self._parameters = parameters
        self._dependencies = dependencies

    @property
    def task_id(self) -> str:
        """
        Returns the task_id
        """
        return self._task_id

    @property
    def operator(self) -> str:
        """
        Returns the operator
        """
        return self._operator

    @operator.setter
    def operator(self, value: str) -> None:
        """
        Sets the operator
        """
        self._operator = value

    @property
    def parameters(self) -> dict:
        """
        Returns the parameters
        """
        return self._parameters

    @parameters.setter
    def parameters(self, value: dict) -> None:
        """
        Sets the parameters
        """
        self._parameters = value

    @property
    def dependencies(self) -> list[str]:
        """
        Returns the dependencies
        """
        return self._dependencies

    @dependencies.setter
    def dependencies(self, value: list[str]) -> None:
        """
        Sets the dependencies
        """
        self._dependencies = value
