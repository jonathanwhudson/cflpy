import os

import pandas as pd
import sqlalchemy as sa

import config

# Should a SQL table be replaced if already exists
IF_EXISTS_FAIL = "fail"
"""Fail attempt if exists"""
IF_EXISTS_REPLACE = "replace"
"""Replace completely if exists"""
IF_EXISTS_APPEND = "append"
"""Add to end if exists"""
IF_EXISTS_CHOICES = {IF_EXISTS_FAIL, IF_EXISTS_REPLACE, IF_EXISTS_APPEND}
"""Summary list for checking if valid argument"""


def mkdir(path_dir: os.path) -> None:
    """
    Make a directory if it does not exist
    :param path_dir: The directory to make
    :return: The file system is changed so the directory now exists
    """
    if not os.path.exists(path_dir):
        os.mkdir(path_dir)


def replace_dataframe(dataframe: pd.DataFrame, table_name: str, datatype: dict = None) -> None:
    """
    Pass on function to store_dataframe but if table exists set to replace
    :param dataframe: The dataframe to store
    :param table_name: The table to name the dataframe as
    :param datatype: The optional assignment of sql types to the table
    :return: None
    """
    store_dataframe(dataframe, table_name, datatype=datatype, if_exists=IF_EXISTS_REPLACE)


def append_dataframe(dataframe: pd.DataFrame, table_name: str, datatype: dict = None) -> None:
    """
    Pass on function to store_dataframe but if table exists set to append to end
    :param dataframe: The dataframe to store
    :param table_name: The table to name the dataframe as
    :param datatype: The optional assignment of sql types to the table
    :return: None
    """
    store_dataframe(dataframe, table_name, datatype=datatype, if_exists=IF_EXISTS_APPEND)


def store_dataframe(dataframe: pd.DataFrame, table_name: str, datatype: dict = None,
                    if_exists: str = IF_EXISTS_FAIL) -> None:
    """
    Store a given dataframe into the database with the given name and optional datatypes
    :param dataframe: The dataframe to store
    :param table_name: The table to name the dataframe as
    :param datatype: The optional assignment of sql types to the table
    :param if_exists:
    :return: None
    """
    if type(dataframe) != pd.DataFrame:
        raise ValueError(f"Type of dataframe <{type(dataframe)}> should be pd.Dataframe!")
    if type(table_name) != str:
        raise ValueError(f"Type of table_name <{type(table_name)}> should be str!")
    if type(if_exists) != str:
        raise ValueError(f"Type of if_exists <{type(if_exists)}> should be str!")
    if datatype and type(datatype) != dict:
        raise ValueError(f"Type of datatype <{type(datatype)}> should be dict!")
    if if_exists not in IF_EXISTS_CHOICES:
        raise ValueError(f"Choice of if_exists <{if_exists}> should be in <{IF_EXISTS_CHOICES}>!")
    # Could be first time we use database so here we will play it safe and create it
    # TODO: Create engine for database once (same with directory)
    mkdir(config.DIR_DATABASE)
    engine = sa.create_engine("sqlite:///" + str(config.FILE_DATABASE))
    with engine.connect() as conn:
        dataframe.to_sql(name=table_name, con=conn, dtype=datatype, if_exists=if_exists, index=False)


def reorder_columns(dataframe: pd.DataFrame, map_column_datatype: dict[str, str]) -> None:
    """
    Set the order of the columns to a preferred one from
    :param dataframe: The dataframe to re-order
    :param map_column_datatype: The map to use for re-ordering (Note rely on new python key-insertion order storage)
    :return: dataframe is re-ordered in place
    """
    if type(dataframe) != pd.DataFrame:
        raise ValueError(f"Type of dataframe <{type(dataframe)}> should be pd.Dataframe!")
    if type(map_column_datatype) != dict:
        raise ValueError(f"Type of map_column_datatype <{type(map_column_datatype)}> should be dict[str,str]!")
    dataframe.reindex(columns=[name for (name, datatype) in map_column_datatype.items()])


def ensure_type_columns(dataframe: pd.DataFrame, map_column_datatype: dict[str:str]) -> None:
    """
    Ensure the existing columns have desired type
    :param dataframe: The dataframe to set type of
    :param map_column_datatype: The map to use for typing
    :return: Columns in dataframe are now typed as desired
    """
    if type(dataframe) != pd.DataFrame:
        raise ValueError(f"Type of dataframe <{type(dataframe)}> should be pd.Dataframe!")
    if type(map_column_datatype) != dict:
        raise ValueError(f"Type of map_column_datatype <{type(map_column_datatype)}> should be dict[str,str]!")
    for column in dataframe.columns:
        if column not in map_column_datatype:
            raise Exception(f"Unexpected column <{column}> in dataframe!")
    for (column, datatype) in map_column_datatype.items():
        # Set type
        if column in dataframe:
            try:
                dataframe[column] = dataframe[column].astype(datatype)
            except (TypeError, ValueError) as e:
                raise ValueError(
                    f"Cannot set column <{column}> datatype which is currently <{dataframe[column].dtype}> to <{datatype}> due to {e}")


def add_missing_columns(dataframe: pd.DataFrame, map_column_datatype: dict[str:str]) -> None:
    """
    Add missing columns with desired type
    :param dataframe: The dataframe add missing columns to
    :param map_column_datatype: The map to use for typing
    :return: Dataframe is now has the missing columns
    """
    if type(dataframe) != pd.DataFrame:
        raise ValueError(f"Type of dataframe <{type(dataframe)}> should be pd.Dataframe!")
    if type(map_column_datatype) != dict:
        raise ValueError(f"Type of map_column_datatype <{type(map_column_datatype)}> should be dict[str,str]!")
    for (column, datatype) in map_column_datatype.items():
        # If column does not exist then create it
        if column not in dataframe:
            dataframe[column] = None
            try:
                dataframe[column] = dataframe[column].astype(datatype)
            except ValueError as e:
                raise ValueError(
                    f"Cannot set newly created column <{column}> datatype which is currently <{dataframe[column].dtype}> to <{datatype}> due to {e}")


def flatten(dataframe: pd.DataFrame, flatten_column) -> pd.DataFrame:
    """
    In a dataframe, take a single column which holds a dictionary and flatten those keys into columns
    Then remove the flattened column
    :param dataframe: The dataframe to flatten
    :param flatten_column: The column to flatten and remove
    :return: The same dataframe with one column replaced by its dictionaries flattened into columns for their key
    """
    if type(dataframe) != pd.DataFrame:
        raise ValueError(f"Type of dataframe <{type(dataframe)}> should be pd.Dataframe!")
    if type(flatten_column) != str and type(flatten_column) != int:
        raise ValueError(f"Type of flatten_column <{type(flatten_column)}> should be str!")
    return pd.concat([dataframe.loc[:, dataframe.columns != flatten_column],
                      pd.DataFrame(dataframe[flatten_column].values.tolist())],
                     axis=1)


def load_dataframe(table_name: str) -> pd.DataFrame:
    """
    Load a dataframe base on table name
    :param table_name: The name of table to load
    :return: The sql table as a dataframe
    """
    if type(table_name) != str:
        raise ValueError(f"Type of table_name <{type(table_name)}> should be str!")
    return load_dataframe_from_query(f"SELECT * from {table_name}")


def load_dataframe_from_query(query_string: str) -> pd.DataFrame:
    """
    Load a dataframe base on query string
    :param query_string: The query to execute
    :return: The result of sql query as a dataframe
    """
    if type(query_string) != str:
        raise ValueError(f"Type of query_string <{type(query_string)}> should be str!")
    engine = sa.create_engine("sqlite:///" + str(config.FILE_DATABASE))
    with engine.connect() as conn:
        return pd.read_sql(sql=query_string, con=conn)


def execute(statement: str) -> None:
    """
    Execute a sql statement
    :param statement: The statement to execute
    :return: None
    """
    if type(statement) != str:
        raise ValueError(f"Type of table_name <{type(statement)}> should be str!")
    engine = sa.create_engine("sqlite:///" + str(config.FILE_DATABASE))
    with engine.connect() as conn:
        conn.execute(statement)
