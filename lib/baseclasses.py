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
    "WriteDisposition",
    "SQLTaskOperator",
    "SQLTask",
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


class SQLTaskOperator(Enum):
    CREATETABLE = "CreateTable"
    TRUNCATETABLE = "TruncateTable"
    DATACHECK = "DataCheck"
    LOADFROMGCS = "LoadFromGCS"
    GCSTOBQ = "GoogleCloudStorageToBigQueryOperator"
    BQCHEK = "BigQueryCheckOperator"
    BQOPERATOR = "BigQueryOperator"


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
        name: str = None,
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


class Delta(object):
    def __init__(self, field: Field, lower_bound: str, upper_bound: int = None) -> None:
        self._field = field
        self._lower_bound = lower_bound
        self._upper_bound = upper_bound

    @property
    def field(self) -> Field:
        """Returns the field"""
        return self._field

    @field.setter
    def field(self, value: Field) -> None:
        """Sets the field"""
        self._field = value

    @property
    def lower_bound(self) -> str:
        """Returns the lower_bound"""
        return self._lower_bound

    @lower_bound.setter
    def lower_bound(self, value: str) -> None:
        """Sets the lower_bound"""
        self._lower_bound = value

    @property
    def upper_bound(self) -> int:
        """Returns the upper_bound"""
        return self._upper_bound

    @upper_bound.setter
    def upper_bound(self, value: int) -> None:
        """Sets the upper_bound"""
        self._upper_bound = value


class History(object):
    def __init__(
        self, partition: Field, driving_column: list[Field], order: list[Field]
    ) -> None:
        self._partition = partition
        self._driving_column = driving_column
        self._order = order

    @property
    def partition(self) -> Field:
        """Returns the partition"""
        return self._partition

    @partition.setter
    def partition(self, value: Field) -> None:
        """Sets the partition"""
        self._partition = value

    @property
    def driving_column(self) -> Field:
        """Returns the driving_column"""
        return self._driving_column

    @driving_column.setter
    def driving_column(self, value: Field) -> None:
        """Sets the driving_column"""
        self._driving_column = value

    @property
    def order(self) -> list[Field]:
        """Returns the order"""
        return self._order

    @order.setter
    def order(self, value: list[Field]) -> None:
        """Sets the order"""
        self._order = value


# > This class is used to create a SQL query that updates a target table with data from a source table
class UpdateTask(object):
    def __init__(
        self,
        target_dataset: str,
        target_table: str,
        source_dataset: str,
        source_table: str,
        source_to_target: list[Field],
        tables: dict,
        where: list[Condition],
    ) -> None:

        self._target_dataset = target_dataset
        self._target_table = target_table
        self._source_dataset = source_dataset
        self._source_table = source_table
        self._source_to_target = source_to_target
        self._tables = tables
        self._where = where

    @property
    def target_dataset(self):
        """
        Returns the target_dataset
        """
        return self._target_dataset

    @target_dataset.setter
    def target_dataset(self, value):
        """
        Sets the target_dataset
        """
        self._target_dataset = value

    @property
    def target_table(self):
        """
        Returns the target_table
        """
        return self._target_table

    @target_table.setter
    def target_table(self, value):
        """
        Sets the target_table
        """
        self._target_table = value

    @property
    def source_dataset(self):
        """
        Returns the source_dataset
        """
        return self._source_dataset

    @source_dataset.setter
    def source_dataset(self, value):
        """
        Sets the source_dataset
        """
        self._source_dataset = value

    @property
    def source_table(self):
        """
        Returns the source_table
        """
        return self._source_table

    @source_table.setter
    def source_table(self, value):
        """
        Sets the source_table
        """
        self._source_table = value

    @property
    def source_to_target(self):
        """
        Returns the source_to_target
        """
        return self._source_to_target

    @source_to_target.setter
    def source_to_target(self, value):
        """
        Sets the source_to_target
        """
        self._source_to_target = value

    @property
    def tables(self):
        """
        Returns the tables
        """
        return self._tables

    @tables.setter
    def tables(self, value):
        """
        Sets the tables
        """
        self._tables = value

    @property
    def where(self):
        """
        Returns the where
        """
        return self._where

    @where.setter
    def where(self, value):
        """
        Sets the where
        """
        self._where = value


class SQLParameter(object):
    def __init__(
        self,
        destination_table: str,
        target_type: int,
        driving_table: str,
        source_to_target: list[Field],
        write_disposition: WriteDisposition,
        sql: str,
        joins: list[Join] = None,
        where: list[Condition] = None,
        delta: Delta = None,
        destination_dataset: str = None,
        history: History = None,
        block_data_check: bool = False,
    ) -> None:
        self._block_data_check = block_data_check
        self._destination_table = destination_table
        self._target_type = target_type
        self._driving_table = driving_table
        self._source_to_target = source_to_target
        self._write_disposition = write_disposition
        self._sql = sql
        self._joins = joins
        self._where = where
        self._delta = delta
        self._destination_dataset = destination_dataset
        self._history = history

    @property
    def block_data_check(self) -> bool:
        """Returns the block_data_check"""
        return self._block_data_check

    @block_data_check.setter
    def block_data_check(self, value: bool) -> None:
        """Sets the block_data_check"""
        self._block_data_check = value

    @property
    def destination_table(self) -> str:
        """Returns the destination_table"""
        return self._destination_table

    @destination_table.setter
    def destination_table(self, value: str) -> None:
        """Sets the destination_table"""
        self._destination_table = value

    @property
    def target_type(self) -> str:
        """Returns the target_type"""
        return self._target_type

    @target_type.setter
    def target_type(self, value: str) -> None:
        """Sets the target_type"""
        self._target_type = value

    @property
    def driving_table(self) -> str:
        """Returns the driving_table"""
        return self._driving_table

    @driving_table.setter
    def driving_table(self, value: str) -> None:
        """Sets the driving_table"""
        self._driving_table = value

    @property
    def source_to_target(self) -> str:
        """Returns the source_to_target"""
        return self._source_to_target

    @source_to_target.setter
    def source_to_target(self, value: str) -> None:
        """Sets the source_to_target"""
        self._source_to_target = value

    @property
    def write_disposition(self) -> str:
        """Returns the write_disposition"""
        return self._write_disposition

    @write_disposition.setter
    def write_disposition(self, value: str) -> None:
        """Sets the write_disposition"""
        self._write_disposition = value

    @property
    def sql(self) -> str:
        """Returns the sql"""
        return self._sql

    @sql.setter
    def sql(self, value: str) -> None:
        """Sets the sql"""
        self._sql = value

    @property
    def joins(self) -> list[Join]:
        """Returns the joins"""
        return self._joins

    @joins.setter
    def joins(self, value: list[Join]) -> None:
        """Sets the joins"""
        self._joins = value

    @property
    def where(self) -> list[Condition]:
        """Returns the where"""
        return self._where

    @where.setter
    def where(self, value: list[Condition]) -> None:
        """Sets the where"""
        self._where = value

    @property
    def delta(self) -> Delta:
        """Returns the delta"""
        return self._delta

    @delta.setter
    def delta(self, value: Delta) -> None:
        """Sets the delta"""
        self._delta = value

    @property
    def destination_dataset(self) -> str:
        """Returns the destination_dataset"""
        return self._destination_dataset

    @destination_dataset.setter
    def destination_dataset(self, value: str) -> None:
        """Sets the destination_dataset"""
        self._destination_dataset = value

    @property
    def history(self) -> History:
        """Returns the history"""
        return self._history

    @history.setter
    def history(self, value: History) -> None:
        """Sets the history"""
        self._history = value


class SQLTask(Task):
    def __init__(
        self,
        task_id: str,
        operator: SQLTaskOperator,
        parameters: SQLParameter,
        dependencies: list[str] = [],
    ) -> None:
        self._task_id = task_id
        self._operator = operator
        self._parameters = parameters
        self._dependencies = dependencies
