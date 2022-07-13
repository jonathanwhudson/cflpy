import os

import config
import create_game_wp_plot
import helper
import store_basic

START_YEAR = 2022
END_YEAR = 2022


def main() -> None:
    helper.mkdir(config.DATA_DIR)
    helper.mkdir(config.DIR_PLOT)
    get_top_10()


def get_top_10(start: int = START_YEAR, end: int = END_YEAR) -> None:
    dir_plots = config.DIR_PLOT.joinpath(f"{start}-{end}")
    helper.mkdir(dir_plots)
    game_ids = get_top_games(top=True, limit=10, start=start, end=end, category="gei")
    save_to_folder(game_ids, dir_plots.joinpath("excitement-high"))
    game_ids = get_top_games(top=False, limit=10, start=start, end=end, category="gei")
    save_to_folder(game_ids, dir_plots.joinpath("excitement-low"))
    game_ids = get_top_games(top=True, limit=10, start=start, end=end, category="gsi")
    save_to_folder(game_ids, dir_plots.joinpath("shootout-high"))
    game_ids = get_top_games(top=False, limit=10, start=start, end=end, category="gsi")
    save_to_folder(game_ids, dir_plots.joinpath("shootout-low"))
    game_ids = get_top_games(top=True, limit=10, start=start, end=end, category="cbf")
    save_to_folder(game_ids, dir_plots.joinpath("comeback-high"))
    game_ids = get_top_games(top=False, limit=10, start=start, end=end, category="cbf")
    save_to_folder(game_ids, dir_plots.joinpath("comeback-low"))


def get_yearly_top_10(start: int = START_YEAR, end: int = END_YEAR) -> None:
    for year in range(end, start - 1, -1):
        dir_plots = config.DIR_PLOT.joinpath(f"{year}")
        helper.mkdir(dir_plots)
        game_ids = get_top_games(top=True, limit=15, start=year, end=year, category="gei")
        save_to_folder(game_ids, dir_plots.joinpath("excitement-high"))
        game_ids = get_top_games(top=False, limit=15, start=year, end=year, category="gei")
        save_to_folder(game_ids, dir_plots.joinpath("excitement-low"))
        game_ids = get_top_games(top=True, limit=15, start=year, end=year, category="gsi")
        save_to_folder(game_ids, dir_plots.joinpath("shootout-high"))
        game_ids = get_top_games(top=False, limit=15, start=year, end=year, category="gsi")
        save_to_folder(game_ids, dir_plots.joinpath("shootout-low"))
        game_ids = get_top_games(top=True, limit=15, start=year, end=year, category="cbf")
        save_to_folder(game_ids, dir_plots.joinpath("comeback-high"))
        game_ids = get_top_games(top=False, limit=15, start=year, end=year, category="cbf")
        save_to_folder(game_ids, dir_plots.joinpath("comeback-low"))


def get_top_games(top: bool = True, limit: int = 15, start: int = 2008, end: int = 2021, type_start: int = 1,
                  type_end: int = 3, category: str = "gei") -> list[int]:
    order = "DESC" if top else "ASC"

    query = f'''
            SELECT gei.game_id 
            FROM gei
            LEFT JOIN games
                ON games.game_id=gei.game_id
            WHERE games.year>={start}
            AND games.year <= {end}
            AND event_type_id >= {type_start}
            AND event_type_id <= {type_end}
            ORDER BY {category} {order}
            LIMIT {limit}
        ;'''
    return store_basic.query(query)['game_id'].values.tolist()


def save_to_folder(game_ids: list[int], folder_name: os.path) -> None:
    helper.mkdir(folder_name)
    i = 1
    for game_id in game_ids:
        plt = create_game_wp_plot.create_plot(game_id)
        file_name = folder_name.joinpath(f"{i:2d}-{game_id}.png")
        plt.savefig(file_name, dpi=300)
        plt.close()
        i += 1


main()
