from enum import Enum

__all__ = ["Condition", "Join"]


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
    LT = "<"
    LE = "<="


class Condition:
    operator = None
    fields = None
    condition = LogicOperator.NONE

    def __init__(
        self, operator: Operator, fields: list, condition: LogicOperator = LogicOperator.NONE
    ) -> None:

        self.operator = operator
        self.fields = fields

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
