import config
import create_game_wp_plot

from download import download_helper
from store import store_games

download_helper.mkdir(config.DATA_DIR)
download_helper.mkdir(config.DIR_PLOT)
game_ids = store_games.extract_games_year_final(config.YEAR_CURRENT)
for (year, game_id) in game_ids:
    plot = create_game_wp_plot.create_plot(game_id)
    dir_plots = config.DIR_PLOT.joinpath(str(config.YEAR_CURRENT))
    download_helper.mkdir(dir_plots)
    filename = dir_plots.joinpath(f"{game_id}.png")
    plot.savefig(filename, dpi=300)
    plot.close()
