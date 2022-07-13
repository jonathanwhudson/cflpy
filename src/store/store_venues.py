import json

import pandas as pd

import config
import store_columns
import store_helper


def store_venues() -> None:
    """
    Store venues from CFL API download
    :return: None
    """
    with open(config.FILE_VENUES) as file:
        venues_df = pd.DataFrame(json.loads(file.read())['data'])
        venues_df.rename(columns={"VenueID": "venue_id", "Name": "venue_name", "Capacity": "venue_capacity"},
                         inplace=True)
        store_helper.add_missing_columns(venues_df, store_columns.VENUES)
        store_helper.ensure_type_columns(venues_df, store_columns.VENUES)
        store_helper.reorder_columns(venues_df, store_columns.VENUES)
        store_helper.replace_dataframe(venues_df, "venues", store_columns.convert_to_sql_types(store_columns.VENUES))


def reset_venues_all() -> None:
    """
    Basically renaming of store_venues as that is a one-shot storage
    :return: None
    """
    store_venues()


def main() -> None:
    """
    Used to test this file
    Resets all venues from download
    :return: None
    """
    reset_venues_all()


def update_with(additional_venues_df: pd.DataFrame) -> None:
    """
    Take a new setup of venues and if any are new to database, then update it
    :param additional_venues_df: A dataframe of correct format to update venues table with
    :return: None
    """
    store_helper.ensure_type_columns(additional_venues_df, store_columns.VENUES)
    store_helper.reorder_columns(additional_venues_df, store_columns.VENUES)
    existing_venues_df = store_helper.load_dataframe("venues")[["venue_id", "venue_name"]]
    store_helper.ensure_type_columns(existing_venues_df, store_columns.VENUES)
    store_helper.reorder_columns(existing_venues_df, store_columns.VENUES)
    difference_df = pd.concat([existing_venues_df, existing_venues_df, additional_venues_df]).drop_duplicates(
        keep=False, subset=["venue_id", "venue_name"])
    if not difference_df.empty:
        store_helper.append_dataframe(difference_df, "venues", store_columns.convert_to_sql_types(store_columns.VENUES))


if __name__ == '__main__':
    main()
    print("Done")
