import json

import pandas as pd

import config
import store_columns
import store_helper
import store_venues


def store_teams() -> None:
    """
    Store teams from CFL API download
    :return: None
    """
    with open(config.FILE_TEAMS) as file:
        teams_df = pd.DataFrame(json.loads(file.read())['data'])
        teams_df = store_helper.flatten(teams_df, "images")
        store_helper.add_missing_columns(teams_df, store_columns.TEAMS)
        store_helper.ensure_type_columns(teams_df, store_columns.TEAMS)
        store_helper.reorder_columns(teams_df, store_columns.TEAMS)
        store_helper.replace_dataframe(teams_df, "teams",
                                       datatype=store_columns.convert_to_sql_types(store_columns.TEAMS))
        venues_df = teams_df.drop_duplicates(subset=["venue_id", "venue_name", "venue_capacity"])[
            ["venue_id", "venue_name", "venue_capacity"]].copy()
        # store_venues.update_with(venues_df)


def reset_teams_all() -> None:
    """
    Basically renaming of store_teams as that is a one-shot storage
    :return: None
    """
    store_teams()


def main() -> None:
    """
    Used to test this file
    Resets all teams from download
    :return: None
    """
    reset_teams_all()


if __name__ == '__main__':
    main()
    print("Done")


def update_with(additional_teams_df: pd.DataFrame) -> None:
    """
    Take a new setup of teams and if any are new to database, then update it
    :param additional_teams_df: A dataframe of correct format to update teams table with
    :return: None
    """
    store_helper.ensure_type_columns(additional_teams_df, store_columns.TEAMS)
    store_helper.reorder_columns(additional_teams_df, store_columns.TEAMS)
    existing_teams_df = store_helper.load_dataframe("teams")[["team_id", "abbreviation", "nickname"]]
    store_helper.ensure_type_columns(existing_teams_df, store_columns.TEAMS)
    store_helper.reorder_columns(existing_teams_df, store_columns.TEAMS)
    difference_df = pd.concat([existing_teams_df, existing_teams_df, additional_teams_df]).drop_duplicates(
        keep=False, subset=["team_id", "abbreviation", "nickname"])
    if not difference_df.empty:
        store_helper.append_dataframe(difference_df, "teams", store_columns.convert_to_sql_types(store_columns.TEAMS))
