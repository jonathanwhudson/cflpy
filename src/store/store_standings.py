import json

import pandas as pd

import config
import store_columns
import store_helper
import store_teams


def load_standings_year_range(start: int, end: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load the standings for given year range
    :param start: The first year
    :param end: The last year
    :return: A tuple of dataframes for the standings and crossover standings
    """
    if type(start) != int:
        raise ValueError(f"Type of start <{type(start)}> should be int!")
    if type(end) != int:
        raise ValueError(f"Type of end <{type(end)}> should be int!")
    years = set(range(start, end + 1, 1))
    if not years:
        raise ValueError(f"Not storing any standings as nothing in years=[{start},{end}]!")
    # First standings
    standings = []
    for year in sorted(years):
        standings.append(load_standings_year(year))
    standings_df = pd.concat(standings, ignore_index=True)
    # Then crossovers
    crossovers = []
    for year in sorted(years):
        if year >= config.YEAR_CROSSOVER_STARTS:
            crossovers.append(load_crossovers_year(year))
    crossover_df = pd.concat(crossovers, ignore_index=True)
    # Ensure in good form
    store_helper.add_missing_columns(standings_df, store_columns.STANDINGS)
    store_helper.ensure_type_columns(standings_df, store_columns.STANDINGS)
    store_helper.reorder_columns(standings_df, store_columns.STANDINGS)
    store_helper.add_missing_columns(crossover_df, store_columns.CROSSOVERS)
    store_helper.ensure_type_columns(crossover_df, store_columns.CROSSOVERS)
    store_helper.reorder_columns(crossover_df, store_columns.CROSSOVERS)
    return standings_df, crossover_df


def load_standings_year(year: int) -> pd.DataFrame:
    """
    Load the standings for the given year
    :param year: The year
    :return: DataFrame of the current standings for given year
    """
    if type(year) != int:
        raise ValueError(f"Type of year <{type(year)}> should be int!")
    file_standings = config.DIR_STANDINGS.joinpath(str(year) + ".json")
    with open(file_standings) as file:
        standings = pd.DataFrame(json.loads(file.read())['data'])
        standings['year'] = year
        return parse_standings(standings)


def load_crossovers_year(year: int) -> pd.DataFrame:
    """
    Load the crossover for the given year
    :param year: The year
    :return: DataFrame of the current crossover for given year
    """
    if type(year) != int:
        raise ValueError(f"Type of year <{type(year)}> should be int!")
    file_crossover = config.DIR_CROSSOVER.joinpath(str(year) + ".json")
    with open(file_crossover) as file:
        crossover = pd.DataFrame(json.loads(file.read())['data'])
        crossover['year'] = year
        return parse_crossovers(crossover)


def parse_standings(standings_df: pd.DataFrame) -> pd.DataFrame:
    """
    Change the rough json DataFrame to a cleaned one
    :param standings_df: The DataFrame to clean
    :return: Cleaned version of DataFrame
    """
    if type(standings_df) != pd.DataFrame:
        raise ValueError(f"Type of standings_df <{type(standings_df)}> should be pd.DataFrame!")
    standings_df = store_helper.flatten(standings_df.reset_index(), "divisions")
    # This data is repeated in each entry, so we can drop it
    # divisions_df = standings_df.loc[:, standings_df.columns != "standings"]
    standings_df = standings_df[['standings']]
    standings_df = store_helper.flatten(standings_df, "standings")
    index = 0
    places = []
    while index in standings_df.columns:
        temp = pd.DataFrame(standings_df[index].dropna().values.tolist())
        temp['place'] = index + 1
        places.append(temp)
        index += 1
    standings_df = pd.concat(places)
    store_helper.add_missing_columns(standings_df, store_columns.STANDINGS)
    store_helper.ensure_type_columns(standings_df, store_columns.STANDINGS)
    store_helper.reorder_columns(standings_df, store_columns.STANDINGS)
    return standings_df


def parse_crossovers(crossover_df: pd.DataFrame) -> pd.DataFrame:
    """
    Change the rough json DataFrame to a cleaned one
    :param crossover_df: The DataFrame to clean
    :return: Cleaned version of DataFrame
    """
    if type(crossover_df) != pd.DataFrame:
        raise ValueError(f"Type of standings_df <{type(crossover_df)}> should be pd.DataFrame!")
    crossover_df = store_helper.flatten(crossover_df.reset_index(), "divisions")
    # This data is repeated in each entry, so we can drop it
    # crossovers_divisions_df = crossover_df.loc[:, crossover_df.columns != "standings"]
    crossover_df.loc[crossover_df['division_name'] == "Crossover", "division_id"] = -1
    crossover_df = store_helper.flatten(crossover_df, 'standings')
    crossover_df.rename(columns={"division_id": "crossover_division_id", "division_name": "crossover_division_name"},
                        inplace=True)
    index = 0
    places = []
    while index in crossover_df.columns:
        temp = pd.DataFrame(crossover_df[['crossover_division_id', 'crossover_division_name', index]].values.tolist(),
                            columns=['crossover_division_id', 'crossover_division_name', index])
        temp['crossover_place'] = index + 1
        temp.dropna(inplace=True)
        temp = store_helper.flatten(temp, index)
        temp.dropna(inplace=True)
        places.append(temp)
        index += 1
    crossover_df = pd.concat(places)
    store_helper.add_missing_columns(crossover_df, store_columns.CROSSOVERS)
    store_helper.ensure_type_columns(crossover_df, store_columns.CROSSOVERS)
    store_helper.reorder_columns(crossover_df, store_columns.CROSSOVERS)
    return crossover_df


def remove_standings_year_range(start: int, end: int) -> None:
    """
    Remove standings for a particular range
    :param start: The first year
    :param end: The last year
    :return: Nothing but database modified to remove standings between those years inclusive
    """
    if type(start) != int:
        raise ValueError(f"Type of start <{type(start)}> should be int!")
    if type(end) != int:
        raise ValueError(f"Type of end <{type(end)}> should be int!")
    years = set(range(start, end + 1, 1))
    if not years:
        raise ValueError(f"Not removing any advanced games as nothing in years=[{start},{end}]!")
    store_helper.execute(f'''DELETE FROM seasons WHERE season in {str(tuple(years)).replace(",)", ")")}''')
    store_helper.execute(f'''DELETE FROM crossovers WHERE season in {str(tuple(years)).replace(",)", ")")}''')


def remove_standings_year(year: int) -> None:
    """
    Remove standings for a particular year
    :param year: The  year
    :return: Nothing but database modified to remove standings for that year
    """
    remove_standings_year_range(year, year)


def reset_standings_all() -> None:
    """
    Reset all the standings
    :return: None
    """
    standings_df, crossover_df = load_standings_year_range(config.YEAR_START_GAMES, config.YEAR_END_GAMES)
    store_helper.replace_dataframe(standings_df, "standings",
                                   datatype=store_columns.convert_to_sql_types(store_columns.STANDINGS))
    store_helper.replace_dataframe(crossover_df, "crossovers",
                                   datatype=store_columns.convert_to_sql_types(store_columns.CROSSOVERS))
    # update_teams(standings_df, crossover_df)


def reset_standings_year(year: int) -> None:
    """
    Reset standings for one year
    :return: None
    """
    standings_df = load_standings_year(year)
    crossover_df = load_crossovers_year(year)
    remove_standings_year(year)
    store_helper.append_dataframe(standings_df, "standings",
                                  datatype=store_columns.convert_to_sql_types(store_columns.STANDINGS))
    store_helper.append_dataframe(crossover_df, "crossovers",
                                  datatype=store_columns.convert_to_sql_types(store_columns.CROSSOVERS))
    # update_teams(standings_df, crossover_df)


def reset_standings_year_current() -> None:
    """
    Reset standings for current year
    :return: None
    """
    reset_standings_year(config.YEAR_CURRENT)


def update_teams(standings_df: pd.DataFrame, crossover_df: pd.DataFrame) -> None:
    """
    Extract the teams from the dataframes and see if they should be added to the team database
    Some of these teams will be secondary entries for the same team_id
    :param standings_df: The standings dataframe
    :param crossover_df: The crossover dataframe
    :return: None
    """
    teams_df = \
        standings_df.drop_duplicates(subset=["team_id", "letter", "abbreviation", "location", "nickname", "full_name"])[
            ["team_id", "letter", "abbreviation", "location", "nickname", "full_name"]].copy()
    store_teams.update_with(teams_df)
    teams_df = \
        crossover_df.drop_duplicates(subset=["team_id", "letter", "abbreviation", "location", "nickname", "full_name"])[
            ["team_id", "letter", "abbreviation", "location", "nickname", "full_name"]].copy()
    store_teams.update_with(teams_df)


def main() -> None:
    """
    Used to test this file
    :return: None
    """
    # Reset standings for all years
    if False:
        reset_standings_all()
    # Reset standings for a specific year
    if False:
        year = 2022
        reset_standings_year(year)
    # Reset standings for current year
    if True:
        reset_standings_year_current()


if __name__ == '__main__':
    main()
    print("Done")
