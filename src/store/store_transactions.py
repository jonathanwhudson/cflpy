import json

import pandas as pd

import config
import store_columns
import store_helper


def load_transactions_year_range(start: int, end: int) -> pd.DataFrame:
    """
    Load the transactions for given year range
    :param start: The first year
    :param end: The last year
    :return: A dataframe of the transactions
    """
    if type(start) != int:
        raise ValueError(f"Type of start <{type(start)}> should be int!")
    if type(end) != int:
        raise ValueError(f"Type of end <{type(end)}> should be int!")
    years = set(range(start, end + 1, 1))
    if not years:
        raise ValueError(f"Not storing any transactions as nothing in years=[{start},{end}]!")
    transactions = []
    for year in sorted(years):
        transactions.append(load_transactions_year(year))
    transactions_df = pd.concat(transactions, ignore_index=True)
    store_helper.add_missing_columns(transactions_df, store_columns.TRANSACTIONS)
    store_helper.ensure_type_columns(transactions_df, store_columns.TRANSACTIONS)
    store_helper.reorder_columns(transactions_df, store_columns.TRANSACTIONS)
    return transactions_df


def load_transactions_year(year: int) -> pd.DataFrame:
    """
    Load the transactions for the given year
    :param year: The year
    :return: DataFrame of the current transactions for given year
    """
    if type(year) != int:
        raise ValueError(f"Type of year <{type(year)}> should be int!")
    file_transactions = config.DIR_TRANSACTIONS.joinpath(str(year) + ".json")
    with open(file_transactions) as file:
        transactions_df = pd.DataFrame(json.loads(file.read())['data'])
        store_helper.add_missing_columns(transactions_df, store_columns.TRANSACTIONS)
        store_helper.ensure_type_columns(transactions_df, store_columns.TRANSACTIONS)
        store_helper.reorder_columns(transactions_df, store_columns.TRANSACTIONS)
        return transactions_df


def remove_transactions_year_range(start: int, end: int) -> None:
    """
    Remove transactions for a particular range
    :param start: The first year
    :param end: The last year
    :return: Nothing but database modified to remove transactions between those years inclusive
    """
    if type(start) != int:
        raise ValueError(f"Type of start <{type(start)}> should be int!")
    if type(end) != int:
        raise ValueError(f"Type of end <{type(end)}> should be int!")
    years = set(range(start, end + 1, 1))
    if not years:
        raise ValueError(f"Not removing any transactions games as nothing in years=[{start},{end}]!")
    store_helper.execute(
        f'''DELETE FROM transactions WHERE strftime('%Y', transaction_date) in {str(tuple(years)).replace(",)", ")")}''')


def remove_transactions_year(year: int) -> None:
    """
    Remove transactions for a particular year
    :param year: The  year
    :return: Nothing but database modified to remove transactions for that year
    """
    if type(year) != int:
        raise ValueError(f"Type of year <{type(year)}> should be int!")
    remove_transactions_year_range(year, year)


def reset_transactions_all() -> None:
    """
    Reset all the transactions
    :return: None
    """
    transactions_df = load_transactions_year_range(config.YEAR_TRANSACTIONS_STARTS, config.YEAR_END_GAMES)
    store_helper.replace_dataframe(transactions_df, "transactions",
                                   datatype=store_columns.convert_to_sql_types(store_columns.TRANSACTIONS))


def reset_transactions_year(year: int) -> None:
    """
    Reset transactions for one year
    :return: None
    """
    transactions_df = load_transactions_year(year)
    remove_transactions_year(year)
    store_helper.append_dataframe(transactions_df, "transactions",
                                  datatype=store_columns.convert_to_sql_types(store_columns.TRANSACTIONS))


def reset_transactions_year_current() -> None:
    """
    Reset transactions for current year
    :return: None
    """
    reset_transactions_year(config.YEAR_CURRENT)


def main() -> None:
    """
    Used to test this file
    :return: None
    """
    # Reset transactions for all years
    if False:
        reset_transactions_all()
    # Reset transactions for a specific year
    if False:
        year = 2022
        reset_transactions_year(year)
    # Reset transactions for current year
    if True:
        reset_transactions_year_current()


if __name__ == '__main__':
    main()
    print("Done")
