import typing
import functools
import toolz
import pandas as pd
import numpy as np
import sqlalchemy as sa
import sqlalchemy.orm as orm
import sqlalchemy.sql.expression as sqlexpression
import sqlalchemy.sql.elements as sqlelements

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
    :param f:
    :param records:
    :return:
    """

    @functools.wraps(f)
    def wrap(*args, **kwargs):
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

    :param table:
    :return:
    """

    if not isinstance(table, sa.Table):
        raise Exception("table argument is not a sqlAlchemy Table")

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

    :param tbl:
    :param k:
    :param val:
    :return:
    """

    if not isinstance(tbl, sa.Table):
        raise Exception("table argument is not a sqlAlchemy Table")

    try:
        if isinstance(val, str) or not isinstance(val, typing.Iterable):
            return tbl.c[k] == val
        return tbl.c[k].in_(val)
    except KeyError as e:
        raise e


def complex_filter_maker(tbl: sa.Table, item: typing.Tuple[str, typing.Any],
                         filter_type: str) -> sqlelements.BinaryExpression:
    """

    :param tbl:
    :param item:
    :param filter_type: one of ('bw', 'lt', 'le', 'gt', 'ge', 'like', 'not_like', 'not_in')
    :return:
    """
    raise NotImplementedError("Yet to be implemented")

    if not isinstance(tbl, sa.Table):
        raise Exception("table argument is not a sqlAlchemy Table")

    key, val = item

    try:
        if filter_type == 'bw':
            if not isinstance(val, tuple):
                raise Exception("Between must be of the form (column_name, (value 1, value 2))")

            return sqlexpression.between(tbl.c[key], val[0], val[1])

        if filter_type == 'lt':
            if isinstance(val, typing.Iterable) and not isinstance(val, str):
                raise Exception("Less than must be of the form (column_name, value)")

            return tbl.c[key] < val

        if filter_type == 'le':
            if isinstance(val, typing.Iterable) and not isinstance(val, str):
                raise Exception("Less than or Equal must be of the form (column_name, value)")

            return tbl.c[key] <= val

        if filter_type == 'gt':
            if isinstance(val, typing.Iterable) and not isinstance(val, str):
                raise Exception("Greater than must be of the form (column_name, value)")

            return tbl.c[key] > val

        if filter_type == 'ge':
            if isinstance(val, typing.Iterable) and not isinstance(val, str):
                raise Exception("Greater than or Equal must be of the form (column_name, value)")

            return tbl.c[key] >= val

        if filter_type == 'like':
            if not isinstance(val, str):
                raise Exception("Like must be of the form (column_name, value)")

            return tbl.c[key].like(val)

        if filter_type == 'not_like':
            if not isinstance(val, str):
                raise Exception("Not Like must be of the form (column_name, value)")

            return tbl.c[key].notlike(val)

        if filter_type == 'not_in':
            if not isinstance(val, tuple):
                raise Exception("Not In must be of the form (column_name, (value 1, value 2, value 3, ..., value n))")
            return tbl.c[key].notin_(val)

    except KeyError as e:
        raise e
