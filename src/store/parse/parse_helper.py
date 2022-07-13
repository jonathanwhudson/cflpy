import pandas as pd


def flatten(dataframe: pd.DataFrame, flatten_column: str) -> pd.DataFrame:
    """
    In a dataframe, take a single column which holds a dictionary and flatten those keys into columns
    Then remove the flattened column
    :param dataframe: The dataframe to flatten
    :param flatten_column: The column to flatten and remove
    :return: The same dataframe with one column replaced by its dictionaries flattened into columns for their key
    """
    return pd.concat([dataframe.loc[:, dataframe.columns != flatten_column],
                      pd.DataFrame(dataframe[flatten_column].values.tolist())],
                     axis=1)


def unroll_2(dataframe: pd.DataFrame, keep: str, unroll: str) -> pd.DataFrame:
    """
    Take a dataframe and for two columns keep both, but unroll the second (take list and make each entry a new row)
    :param dataframe: The dataframe to work on
    :param keep: The columns to keep
    :param unroll: The column to unroll
    :return: A dataframe keep first column, and unroll second
    """
    columns = [keep, unroll]
    return pd.DataFrame([[a, c] for a, b in dataframe[columns].values for c in b], columns=columns)


def unroll_4(dataframe: pd.DataFrame, keep: list[str], unroll: str) -> pd.DataFrame:
    """
    Take a dataframe and for 4 columns keep 3, but unroll the fourth (take list and make each entry a new row)
    :param dataframe: The dataframe to work on
    :param keep: The columns to keep
    :param unroll: The column to unroll
    :return: A dataframe keep first three columns, and unroll fourth
    """
    columns = keep + [unroll]
    return pd.DataFrame([[a, b, c, e] for a, b, c, d in dataframe[columns].values for e in d], columns=columns)
