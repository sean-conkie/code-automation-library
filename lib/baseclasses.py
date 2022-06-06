from enum import Enum

__all__ = ["Condition", "Field", "Task", "LogicOperator" "Join"]


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


class WriteDisposition(Enum):
    WRITE_APPEND = "WRITE_APPEND"
    WRITE_TRANSIENT = "WRITE_TRANSIENT"
    WRITE_TRUNCATE = "WRITE_TRUNCATE"


class Condition:
    fields = None
    condition = LogicOperator.NONE
    operator = Operator.NONE

    def __init__(
        self,
        fields: list[str],
        condition: LogicOperator = LogicOperator.NONE,
        operator: Operator = Operator.NONE,
    ) -> None:

        self.fields = fields
        self.condition = condition
        self.operator = operator

    def __str__(self) -> str:
        return f"{self.fields[0]} {self.operator.value} {self.fields[1]}"


class Join:
    left = None
    right = None
    on = []

    def __init__(self, right: str, on: list[Condition], left: str = None) -> None:

        self.left = left
        self.right = right
        self.on = on


class Field:
    name = None
    source_column = None
    source_name = None
    transformation = None
    pk = None
    hk = None

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
            self.transformation = transformation
        elif source_column:
            self.source_column = source_column
            self.source_name = source_name
        else:
            raise ValueError(
                "Either 'transformation' or 'source_name' must be provided."
            )

        self.name = name
        self.pk = pk
        self.hk = hk


class Task:

    task_id = None
    operator = None
    parameters = {}
    dependencies = []

    def __init__(
        self,
        task_id: str,
        operator: str,
        parameters: dict,
        dependencies: list = [],
    ) -> None:
        self.task_id = task_id
        self.operator = operator
        self.parameters = parameters
        self.dependencies = dependencies
