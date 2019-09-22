import typing
import functools
import toolz
import pandas as pd
import numpy as np
import sqlalchemy as sa
import sqlalchemy.orm as orm
import sqlalchemy.sql.expression as sqlexpression
import sqlalchemy.sql.elements as sqlelements
from .exceptions_ import BadArgumentType, NoSuchColumn

function_type_for_dframe = typing.Callable[..., typing.Tuple[np.ndarray, typing.Tuple[str, ...]]]
regular_column_content = typing.Union[str, int, float, tuple, dict]
inspect_type = typing.Dict[
    str,
    typing.Union[
        str,
        typing.List[typing.Dict[str, typing.Union[str, bool, type, None, sa.types.TypeEngine]]]
    ]
]


@toolz.curry
def d_frame(f: function_type_for_dframe, records: bool = False) -> typing.Callable[..., pd.DataFrame]:
    """
    Decorator that produces a pandas DataFrame from numpy array and columns
    :param f: a function that returns either a tuple of numpy arrau and column names or a tuple of records (dictionaries)
    :param records: True if the function supplied returns tuples of records, False if ndarray and columns
    :return:
    """

    @functools.wraps(f)
    def wrap(*args, **kwargs) -> pd.DataFrame:
        if not records:
            arr, cols = f(*args, **kwargs)
            if arr.size == 0:
                return pd.DataFrame(columns=cols, copy=True)
            return pd.DataFrame(data=arr, columns=cols, copy=True)

        records_ = f(*args, **kwargs)
        return pd.DataFrame().from_records(records_)

    return wrap


def inspect_table(table: sa.Table) -> inspect_type:
    """

    :param table: a sqlalchemy Table object
    :return: a dictionary with some metdata on the table
    """

    if not isinstance(table, sa.Table):
        raise BadArgumentType("table argument is not a sqlAlchemy Table", None)

    def str_type(typ: sa.types.TypeEngine) -> typing.Union[str, sa.types.TypeEngine]:
        """

        :param typ:
        :return:
        """

        try:
            return str(typ)
        except Exception as _:
            return typ

    def python_type_(typ: sa.types.TypeEngine) -> typing.Union[type, None]:
        """

        :param typ:
        :return:
        """

        try:
            return typ.python_type
        except Exception as _:
            return None

    # number of rows
    if isinstance(table.bind, sa.engine.base.Engine):
        try:
            number_of_rows = orm.Session(table.bind).query(table).count()
        except Exception as _:
            number_of_rows = "N/A"
    else:
        number_of_rows = "N/A"

    column_info = [
        dict(
            column_name=str(el.name),
            column_type=str_type(el.type),
            python_type=python_type_(el.type),
            primary_key=el.primary_key,
            nullable=el.nullable,
        )
        for el in table.columns
    ]

    schema = table.schema

    return dict(
        table_name=str(table.name),
        row_count=number_of_rows,
        schema=None if schema is None else str(schema),
        columns=column_info
    )


def filter_maker(tbl: sa.Table, k: str, val: regular_column_content) -> sqlelements.BinaryExpression:
    """

    :param tbl: a sqlalchemy Table object
    :param k: a column name in that table object
    :param val: a value we want that column to be. Single value for k==val and a tuple means k.in_(val)
    :return:
    """

    if not isinstance(tbl, sa.Table):
        raise Exception("table argument is not a sqlAlchemy Table")

    try:
        if isinstance(val, str) or not isinstance(val, typing.Iterable):
            return tbl.c[k] == val
        return tbl.c[k].in_(val)
    except KeyError as e:
        raise NoSuchColumn(f"{k} is not a column in the {tbl.name} table", e)


def complex_filter_maker(tbl: sa.Table, item: typing.Tuple[str, typing.Any],
                         filter_type: str) -> sqlelements.BinaryExpression:
    """

    :param tbl:
    :param item:
    :param filter_type: one of ('bw', 'lt', 'le', 'gt', 'ge', 'like', 'not_like', 'not_in')
    :return:
    """
    raise NotImplementedError("Yet to be implemented")
