from enum import Enum, auto
from typing import List, Optional, Union
from dataclasses import dataclass


@dataclass
class Field:
    column: str
    aggregation: Optional[str] = None


class SQLOperator(Enum):
    EQ = auto()
    GOE = auto()
    GT = auto()
    LOE = auto()
    LT = auto()
    IN = auto()
    NOT_IN = auto()


@dataclass
class Condition:
    field: Field
    operator: Union[SQLOperator, str]
    value: List[Union[str, float]]
    type: Optional[str] = None


class SQLSorting(Enum):
    DESC = auto()
    ASC = auto()


@dataclass
class Sorting:
    field: str
    order: SQLSorting


@dataclass
class Setting:
    type: str


@dataclass
class Component:
    type: str
    setting: Optional[List[Setting]] = None


@dataclass
class GroupElement:
    element: str


@dataclass
class From:
    table: str


@dataclass
class Query:
    fields: List[Field]
    froms: Optional[List[From]] = None
    where: Optional[List[Condition]] = None
    groupBy: Optional[List[GroupElement]] = None
    orderBy: Optional[List[Sorting]] = None
    limit: Optional[int] = None

    def to_sql(self):
        sql = "SELECT {} FROM {}"

        fields_with_agg = []
        for field in self.fields:
            if field.aggregation is not None:
                field_with_agg = field.aggregation + " ( " + field.column + " )"
            else:
                field_with_agg = field.column

            fields_with_agg.append(field_with_agg)

        formatted_fields = ", ".join(fields_with_agg)

        froms_array = []
        for f in self.froms:
            froms_array.append(f.table)
        table = ", ".join(froms_array)

        sql = sql.format(formatted_fields, table)

        where_conditions = []
        if self.where is not None:
            sql_where = " WHERE {}"

            for condition in self.where:
                formatted_value = "( " + ", ".join(condition.value) + " )"
                if isinstance(condition.operator, str):
                    operator = condition.operator
                else:
                    operator = condition.operator.name
                    if operator == "GOE":
                        operator = ">="
                    elif operator == "LOE":
                        operator = "<="
                    elif operator == "EQ":
                        operator = "=="
                where_condition = (
                    condition.field.column + " " + operator + " " + str(formatted_value)
                )
                where_conditions.append(where_condition)

            formatted_where_conditions = " AND ".join(where_conditions)
            sql_where = sql_where.format(formatted_where_conditions)
            sql += sql_where

        group_conditions = []
        if self.groupBy is not None:
            sql_groupBy = " GROUP BY {}"

            for group in self.groupBy:
                group_conditions.append(group.element)
            formatted_group = ", ".join(group_conditions)
            sql_groupBy = sql_groupBy.format(formatted_group)
            sql += sql_groupBy

        sorting_conditions = []
        if self.orderBy is not None:
            sql_orderby = " ORDER BY {}"

            for sorting in self.orderBy:
                sort_order = sorting.order.name
                sort_condition = sorting.field + " " + sort_order
                sorting_conditions.append(sort_condition)

            formatted_sorting = ", ".join(sorting_conditions)
            sql_orderby = sql_orderby.format(formatted_sorting)
            sql += sql_orderby

        if self.limit is not None:
            sql += " LIMIT " + str(self.limit)

        return sql


@dataclass
class SmartQuery:
    queries: List[Query]
    chart: Optional[str] = None  # no more used
    components: Optional[List[Component]] = None  # use this instead of chart
    javascript: Optional[List[str]] = None
