import json

import pandas as pd

import config
import store_columns
import store_helper


def store_seasons() -> None:
    """
    Store seasons from CFL API download
    :return: None
    """
    with open(config.FILE_SEASONS) as file:
        season_df = pd.DataFrame(json.loads(file.read())['data']['seasons'])
        season_df = store_helper.flatten(season_df, "preseason")
        season_df.rename(columns={'start': 'preseason_start', 'end': 'preseason_end'}, inplace=True)
        season_df = store_helper.flatten(season_df, "regular_season")
        season_df.rename(columns={'start': 'regular_season_start', 'end': 'regular_season_end'}, inplace=True)
        season_df = store_helper.flatten(season_df, "semifinals")
        season_df.rename(columns={'start': 'semifinals_start', 'end': 'semifinals_end'}, inplace=True)
        season_df = store_helper.flatten(season_df, "finals")
        season_df.rename(columns={'start': 'finals_start', 'end': 'finals_end'}, inplace=True)
        season_df = store_helper.flatten(season_df, "grey_cup")
        season_df.rename(columns={'start': 'grey_cup_start', 'end': 'grey_cup_end'}, inplace=True)
        store_helper.ensure_type_columns(season_df, store_columns.SEASONS)
        store_helper.reorder_columns(season_df, store_columns.SEASONS)
        store_helper.replace_dataframe(season_df, "seasons",
                                       datatype=store_columns.convert_to_sql_types(store_columns.SEASONS))


def reset_seasons_all() -> None:
    """
    Basically renaming of store_seasons as that is a one-shot storage
    :return: None
    """
    store_seasons()


def main() -> None:
    """
    Used to test this file
    Resets all seasons from download
    :return: None
    """
    reset_seasons_all()


if __name__ == '__main__':
    main()
    print("Done")


def update_with(additional_seasons_df: pd.DataFrame) -> None:
    """
    Take a new setup of seasons and if any are new to database, then update it
    :param additional_seasons_df: A dataframe of correct format to update seasons table with
    :return: None
    """
    store_helper.ensure_type_columns(additional_seasons_df, store_columns.SEASONS)
    store_helper.reorder_columns(additional_seasons_df, store_columns.SEASONS)
    store_helper.add_missing_columns(additional_seasons_df, store_columns.SEASONS)
    existing_seasons_df = store_helper.load_dataframe("seasons")
    store_helper.ensure_type_columns(existing_seasons_df, store_columns.SEASONS)
    store_helper.reorder_columns(existing_seasons_df, store_columns.SEASONS)
    difference_df = pd.concat([existing_seasons_df, existing_seasons_df, additional_seasons_df]).drop_duplicates(
        keep=False)
    if not difference_df.empty:
        store_helper.append_dataframe(difference_df, "seasons",
                                      store_columns.convert_to_sql_types(store_columns.SEASONS))
