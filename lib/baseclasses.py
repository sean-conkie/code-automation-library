import json
import re

from enum import Enum
from typing import Union
from unittest.util import strclass
from warnings import warn

from lib.helper import ifnull, isnullorwhitespace

__all__ = [
    "Condition",
    "Field",
    "Task",
    "LogicOperator",
    "Join",
    "Operator",
    "JoinType",
    "WriteDisposition",
    "TaskOperator",
    "SQLTask",
    "SQLDataCheckTask",
    "SQLParameter",
    "SQLDataCheckParameter",
    "todict",
    "converttoobj",
    "TableType",
]


class ConversionType(Enum):
    WHERE = "where"
    ANALYTIC = "analytic"
    JOIN = "join"
    DELTA = "delta"
    SOURCE = "source_to_target"


class LogicOperator(Enum):

    AND = "and"
    OR = "or"
    NOT = "not"
    NONE = None


class AnalyticType(Enum):
    LAG = "lag"
    LEAD = "lead"
    ROWNUM = "row_number"
    RANK = "rank"
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
    WRITEAPPEND = "WRITE_APPEND"
    WRITETRANSIENT = "WRITE_TRUNCATE"
    WRITETRUNCATE = "WRITE_APPEND"
    DELETE = "DELETE"


class TaskOperator(Enum):
    CREATETABLE = "CreateTable"
    TRUNCATETABLE = "TruncateTable"
    DATACHECK = "DataCheck"
    LOADFROMGCS = "LoadFromGCS"
    GCSTOBQ = "GoogleCloudStorageToBigQueryOperator"
    BQCHEK = "BigQueryCheckOperator"
    BQOPERATOR = "BigQueryOperator"
    EXTSENSOR = "ExternalTaskSensor"


class TableType(Enum):
    TYPE1 = 1
    HISTORY = 2
    TYPE3 = 3
    TYPE4 = 4
    TYPE5 = 5
    TYPE6 = 6


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
        return str(todict(self))

    def __repr__(self):
        return str(self)

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

    def __str__(self) -> str:
        return str(todict(self))

    def __repr__(self):
        return str(self)

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
        data_type: str = None,
        source_column: str = None,
        source_name: str = None,
        transformation: str = None,
        nullable: bool = None,
        pk: bool = None,
        hk: bool = None,
    ) -> None:

        self._transformation = transformation
        self._source_column = source_column
        self._source_name = source_name
        self._name = name
        self._data_type = data_type
        self._nullable = nullable
        self._pk = pk
        self._hk = hk

    def __str__(self) -> str:
        return str(todict(self))

    def __repr__(self):
        return str(self)

    @property
    def name(self) -> str:
        """
        If the name is null or whitespace, then if the source column is null or whitespace and the
        transformation is not null or whitespace, then if the transformation contains a cast column and
        cast type, then return the cast column and cast type, else if the transformation contains a
        decode, then return the decode, else if the transformation contains a function, then return the
        function, else return the source column, else return the name

        Returns:
          The name of the column
        """
        if isnullorwhitespace(self._name):
            if isnullorwhitespace(self._source_column) and not isnullorwhitespace(
                self._transformation
            ):
                regex = r"(?:(?P<function>\b\w+\b)?\()?(?:(?P<cast_col>\w+) as (?P<cast_type>\w+))?(?P<decode>case)?"
                m = re.search(regex, self._transformation, re.IGNORECASE)
                if m:
                    if not isnullorwhitespace(
                        m.group("cast_col")
                    ) and not isnullorwhitespace(m.group("cast_type")):
                        return f"{m.group('cast_col')}_{m.group('cast_type')}"
                    elif not isnullorwhitespace(m.group("decode")):
                        return m.group("decode")
                    elif not isnullorwhitespace(m.group("function")):
                        return m.group("function")
            else:
                return self._source_column

        return self._name

    @name.setter
    def name(self, value: str) -> None:
        """
        Sets the name
        """
        self._name = value

    @property
    def data_type(self) -> str:
        """
        It returns the data type of the object

        Returns:
          The data type of the variable.
        """
        return self._data_type

    @data_type.setter
    def data_type(self, value: str) -> None:
        """
        This function takes in a string and returns None

        Args:
          value (str): The value to be converted to the specified data type.
        """
        self._data_type = value

    @property
    def source_column(self) -> str:
        """
        If the source column is null or whitespace, and the transformation is null or whitespace, then
        return the name of the column

        Returns:
          The source column name.
        """
        if isnullorwhitespace(self._source_column) and isnullorwhitespace(
            self._transformation
        ):
            return self._name

        return self._source_column

    @source_column.setter
    def source_column(self, value: str) -> None:
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
    def nullable(self) -> bool:
        if self._nullable is None:
            return True
        return self._nullable

    @nullable.setter
    def nullable(self, value: bool) -> None:
        """
        Sets the nullable
        """
        self._nullable = value

    @property
    def pk(self) -> bool:
        """
        It returns a boolean value.

        Returns:
          The value of the _pk attribute.
        """
        if self._pk is None:
            return False
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
        It returns a boolean value.

        Returns:
          The value of the attribute _hk.
        """
        if self._hk is None:
            return False
        return self._hk

    @hk.setter
    def hk(self, value: bool) -> None:
        """
        Sets the hk
        """
        self._hk = value

    def source(self, default_source_name: str = None) -> str:

        if isnullorwhitespace(self._transformation):
            if isnullorwhitespace(self._source_name) and isnullorwhitespace(
                default_source_name
            ):
                return self.source_column
            return (
                f"{ifnull(self._source_name, default_source_name)}.{self.source_column}"
            )
        else:
            return self._transformation


class Task(object):
    def __init__(
        self,
        task_id: str,
        operator: str,
        parameters: dict,
        author: str,
        dependencies: list[str] = [],
        description=None,
    ) -> None:
        self._task_id = task_id
        self._operator = operator
        self._parameters = parameters
        self._dependencies = dependencies
        self._description = description
        self._author = author

    def __str__(self) -> str:
        return json.dumps(todict(self), indent=4, sort_keys=True)

    def __repr__(self):
        return str(self)

    @property
    def task_id(self) -> str:
        """
        Returns the task_id
        """
        return self._task_id

    @property
    def description(self) -> str:
        """
        Returns the description
        """
        return self._description

    @property
    def author(self) -> str:
        """Returns the author"""
        return self._author

    @author.setter
    def author(self, value: str) -> None:
        """Sets the author"""
        self._author = value

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

    def __str__(self) -> str:
        return str(todict(self))

    def __repr__(self):
        return str(self)

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


class Analytic(object):
    def __init__(
        self,
        partition: list[Field],
        order: list[Field],
        type: AnalyticType = AnalyticType.NONE,
        driving_column: list[Field] = None,
        column: Field = None,
        offset: int = None,
        default: str = None,
    ) -> None:
        self._partition = partition
        self._driving_column = driving_column
        self._order = order
        self._type = type
        self._column = column
        self._offset = offset
        self._default = default

    def __str__(self) -> str:
        return str(todict(self))

    def __repr__(self):
        return str(self)

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

    @property
    def type(self) -> AnalyticType:
        """Returns the type"""
        return self._type

    @type.setter
    def type(self, value: AnalyticType) -> None:
        """Sets the type"""
        self._type = value

    @property
    def column(self) -> Field:
        """Returns the column"""
        return self._column

    @column.setter
    def column(self, value: Field) -> None:
        """Sets the column"""
        self._column = value

    @property
    def offset(self) -> int:
        """Returns the offset"""
        return self._offset

    @offset.setter
    def offset(self, value: int) -> None:
        """Sets the offset"""
        self._offset = value

    @property
    def default(self) -> str:
        """Returns the default"""
        return self._default

    @default.setter
    def default(self, value: str) -> None:
        """Sets the default"""
        self._default = value


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

    def __str__(self) -> str:
        return str(todict(self))

    def __repr__(self):
        return str(self)

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
    def source_to_target(self) -> list[Field]:
        """
        Returns the source_to_target
        """
        return self._source_to_target

    @source_to_target.setter
    def source_to_target(self, value: list[Field]):
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
        target_type: TableType,
        driving_table: str,
        source_to_target: list[Field],
        write_disposition: WriteDisposition,
        sql: str = None,
        joins: list[Join] = None,
        where: list[Condition] = None,
        delta: Delta = None,
        destination_dataset: str = None,
        staging_dataset: str = None,
        history: Analytic = None,
        block_data_check: bool = False,
        build_artifacts: bool = True,
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
        self._staging_dataset = staging_dataset
        self._history = history
        self._build_artifacts = build_artifacts

    def __str__(self) -> str:
        return str(todict(self))

    def __repr__(self):
        return str(self)

    @property
    def block_data_check(self) -> bool:
        """Returns the block_data_check"""
        return self._block_data_check

    @block_data_check.setter
    def block_data_check(self, value: bool) -> None:
        """Sets the block_data_check"""
        self._block_data_check = value

    @property
    def build_artifacts(self) -> bool:
        """Returns the build_artifacts"""
        return self._build_artifacts

    @build_artifacts.setter
    def build_artifacts(self, value: bool) -> None:
        """Sets the build_artifacts"""
        self.build_artifacts = value

    @property
    def target_type(self) -> TableType:
        """Returns the target_type"""
        return self._target_type

    @target_type.setter
    def target_type(self, value: TableType) -> None:
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
    def source_to_target(self) -> list[Field]:
        """Returns the source_to_target"""
        return self._source_to_target

    @source_to_target.setter
    def source_to_target(self, value: list[Field]) -> None:
        """Sets the source_to_target"""
        self._source_to_target = value

    @property
    def write_disposition(self) -> WriteDisposition:
        """Returns the write_disposition"""
        return self._write_disposition

    @write_disposition.setter
    def write_disposition(self, value: WriteDisposition) -> None:
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
    def destination_table(self) -> str:
        """Returns the destination_table"""
        return self._destination_table

    @destination_table.setter
    def destination_table(self, value: str) -> None:
        """Sets the destination_table"""
        self._destination_table = value

    @property
    def staging_dataset(self) -> str:
        """Returns the staging_dataset"""
        return self._staging_dataset

    @staging_dataset.setter
    def staging_dataset(self, value: str) -> None:
        """Sets the staging_dataset"""
        self._staging_dataset = value

    @property
    def history(self) -> Analytic:
        """Returns the history"""
        return self._history

    @history.setter
    def history(self, value: Analytic) -> None:
        """Sets the history"""
        self._history = value


class SQLTask(Task):
    def __init__(
        self,
        task_id: str,
        operator: TaskOperator,
        parameters: SQLParameter,
        author: str,
        dependencies: list[str] = [],
        description=None,
    ) -> None:
        super().__init__(
            task_id, operator, parameters, author, dependencies, description
        )

    @property
    def author(self) -> str:
        """Returns the author"""
        return self._author

    @author.setter
    def author(self, value: str) -> None:
        """Sets the author"""
        self._author = value

    @property
    def operator(self) -> TaskOperator:
        """
        Returns the operator
        """
        return self._operator

    @property
    def description(self) -> str:
        """
        Returns the description
        """
        return self._description

    @operator.setter
    def operator(self, value: TaskOperator) -> None:
        """
        Sets the operator
        """
        self._operator = value

    @property
    def parameters(self) -> SQLParameter:
        """
        Returns the parameters
        """
        return self._parameters

    @parameters.setter
    def parameters(self, value: SQLParameter) -> None:
        """
        Sets the parameters
        """
        self._parameters = value

    @property
    def history_keys(self) -> list[str]:
        """
        > This function returns a list of strings that are the names of the fields that are history keys

        Returns:
          A list of strings.
        """

        return [field.name for field in self.parameters.source_to_target if field.hk]

    @property
    def primary_keys(self) -> list[str]:
        """
        It returns a list of strings, where each string is the name of a field in the source table that
        is a primary key.

        Returns:
          A list of strings.
        """

        return [field.name for field in self.parameters.source_to_target if field.pk]

    def __createfieldlist(self, fields: list[Field]) -> str:
        """
        It takes a list of fields and returns a string of comma separated field names

        Args:
          fields (list[Field]): list of Field objects

        Returns:
          A string of the fields in the field list.
        """
        field_list = []
        for p in fields:
            source_name = (
                p.source_name if p.source_name else self.parameters.driving_table
            )
            source_column = p.source_column
            field_list.append(
                f"{source_name}.{source_column}"
                if source_column
                else f"{p.transformation}"
            )
        return ",".join(field_list)

    def add_analytic(self, analytic: Analytic) -> None:
        """
        > This function adds an analytic to the source_to_target list

        Args:
          analytic (Analytic): Analytic object

        Returns:
          None
        """

        partition = self.__createfieldlist(analytic.partition)
        order = self.__createfieldlist(analytic.order)

        source_name = (
            analytic.column.source_name
            if analytic.column.source_name
            else self.parameters.driving_table
        )
        source_column = analytic.column.name
        offset = f", {analytic.offset}" if analytic.offset else ""
        default = f", {analytic.default}" if analytic.default else ""

        analytic_transformation = Field(
            name=analytic.column.name,
            transformation=f"""{analytic.type.value}({source_name}.{source_column}{offset}{default}) 
            over(partition by {partition} 
                     order by {order})""",
        )

        column_list = [c.name for c in self.parameters.source_to_target]
        if analytic_transformation.name in column_list:
            for i, c in enumerate(self.parameters.source_to_target):
                if analytic_transformation.name == c.name:
                    self.parameters.source_to_target[i] = analytic_transformation
                    break
        else:
            self.parameters.source_to_target.append(analytic_transformation)

        return None


class SQLDataCheckParameter(object):
    def __init__(self, sql: str, params: dict = None) -> None:
        self._sql = sql
        self._params = params

    def __str__(self) -> str:
        return str(todict(self))

    def __repr__(self):
        return str(self)

    @property
    def sql(self) -> str:
        """Returns the sql"""
        return self._sql

    @sql.setter
    def sql(self, value: str) -> None:
        """Sets the sql"""
        self._sql = value

    @property
    def params(self) -> dict:
        """Returns the params"""
        return self._params

    @params.setter
    def params(self, value: dict) -> None:
        """Sets the params"""
        self._params = value


class SQLDataCheckTask(Task):
    def __init__(
        self,
        task_id: str,
        operator: TaskOperator,
        parameters: SQLDataCheckParameter,
        dependencies: list[str] = [],
    ) -> None:
        super().__init__(task_id, operator, parameters, dependencies)

    @property
    def operator(self) -> TaskOperator:
        """
        Returns the operator
        """
        return self._operator

    @operator.setter
    def operator(self, value: TaskOperator) -> None:
        """
        Sets the operator
        """
        self._operator = value

    @property
    def parameters(self) -> SQLDataCheckParameter:
        """
        Returns the parameters
        """
        return self._parameters

    @parameters.setter
    def parameters(self, value: SQLDataCheckParameter) -> None:
        """
        Sets the parameters
        """
        self._parameters = value


def todict(obj, classkey=None):
    """
    It converts an object to a dictionary, and if the object is a class, it converts the class to a
    dictionary, and if the class has a class, it converts that class to a dictionary, and so on

    Args:
      obj: The object to convert to a dictionary.
      classkey: If this is provided, the resulting dictionary will include a key for the class name of
    the object.

    Returns:
      A dictionary of the object's attributes.
    """
    if isinstance(obj, dict):
        data = {}
        for (k, v) in obj.items():
            data[k] = todict(v, classkey)
        return data
    elif hasattr(obj, "_ast"):
        return todict(obj._ast())
    elif hasattr(obj, "__iter__") and not isinstance(obj, str):
        return [todict(v, classkey) for v in obj]
    elif issubclass(type(obj), Enum):
        return obj.name
    elif hasattr(obj, "__dict__"):
        data = dict(
            [
                (re.sub(r"^_", "", key, re.IGNORECASE), todict(value, classkey))
                for key, value in obj.__dict__.items()
                if not callable(value)
            ]
        )
        if classkey is not None and hasattr(obj, "__class__"):
            data[classkey] = obj.__class__.__name__
        return data
    else:
        return obj


def converttoobj(
    input: Union[list, dict],
    conversiontype: ConversionType,
) -> Union[Analytic, Delta, list[Join], list[Condition], list[Field]]:
    """
    It takes a list or dictionary and converts it to a list of Join, Condition, or Field objects

    Args:
      input (Union[list, dict]): The input to be converted.
      conversiontype (ConversionType): The type of object you want to convert to.

    Returns:
      An object based on the conversiontype supplied (Analytic, Delta, list[Join], list[Condition],
      list[Field])
    """

    if not input:
        return None
    elif conversiontype in [
        ConversionType.JOIN,
        ConversionType.WHERE,
        ConversionType.SOURCE,
    ]:
        if not type(input) == list:
            warn(
                f"A list input must be provided for conversion to {conversiontype.value}"
            )
            input = [input]
    elif conversiontype in [ConversionType.ANALYTIC, ConversionType.DELTA]:
        if not type(input) == dict:
            raise ValueError(
                f"A dictionary input must be provided for conversion to {conversiontype.value}"
            )

    if conversiontype == ConversionType.ANALYTIC:
        obj = Analytic(
            [
                Field(
                    name=field.get("name"),
                    source_column=field.get("source_column"),
                    source_name=field.get("source_name"),
                    transformation=field.get("transformation"),
                    pk=field.get("pk"),
                    hk=field.get("hk"),
                )
                for field in input.get("partition")
            ],
            [
                Field(
                    name=field.get("name"),
                    source_column=field.get("source_column"),
                    source_name=field.get("source_name"),
                    transformation=field.get("transformation"),
                    pk=field.get("pk"),
                    hk=field.get("hk"),
                )
                for field in input.get("order")
            ],
            AnalyticType(input.get("type")),
            [
                Field(
                    name=field.get("name"),
                    source_column=field.get("source_column"),
                    source_name=field.get("source_name"),
                    transformation=field.get("transformation"),
                    pk=field.get("pk"),
                    hk=field.get("hk"),
                )
                for field in input.get("driving_column")
            ],
            Field(
                name=input.get("column", {}).get("name"),
                source_column=input.get("column", {}).get("source_column"),
                source_name=input.get("column", {}).get("source_name"),
                transformation=input.get("column", {}).get("transformation"),
                pk=input.get("column", {}).get("pk"),
                hk=input.get("column", {}).get("hk"),
            ),
            input.get("offset"),
            input.get("default"),
        )

    elif conversiontype == ConversionType.DELTA:
        field = input.get("field", {})
        obj = Delta(
            Field(
                name=field.get("name"),
                source_column=field.get("source_column"),
                source_name=field.get("source_name"),
                transformation=field.get("transformation"),
                pk=field.get("pk"),
                hk=field.get("hk"),
            ),
            input.get("lower_bound"),
            upper_bound=input.get("upper_bound"),
        )

    elif conversiontype == ConversionType.JOIN:
        obj = [
            Join(
                j.get("right"),
                [
                    Condition(
                        [field for field in c.get("fields", [])],
                        operator=Operator(c.get("operator", "=")),
                        condition=LogicOperator(c.get("condition", "and").lower()),
                    )
                    for c in j.get("on", [])
                ],
                j.get("left"),
                JoinType(j.get("type", "left").lower()),
            )
            for j in input
        ]

    elif conversiontype == ConversionType.SOURCE:
        obj = [
            Field(
                name=field.get("name"),
                data_type=field.get("data_type"),
                source_column=field.get("source_column"),
                source_name=field.get("source_name"),
                transformation=field.get("transformation"),
                nullable=field.get("nullable"),
                pk=field.get("pk"),
                hk=field.get("hk"),
            )
            for field in input
        ]
    elif conversiontype == ConversionType.WHERE:
        obj = [
            Condition(
                [field for field in c.get("fields", [])],
                operator=Operator(c.get("operator", "=")),
                condition=LogicOperator(c.get("condition", "and").lower()),
            )
            for c in input
        ]

    return obj
