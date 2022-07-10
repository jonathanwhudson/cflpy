
import pandas as pd

import config
import store

pbp_df = store.query(f"""SELECT epa.*,games.team_1_score,games.team_2_score,drives.won FROM epa LEFT JOIN drives ON drives.play_id=epa.play_id and drives.entry=epa.entry LEFT JOIN games ON games.game_id=epa.game_id WHERE epa.year >= {config.YEAR_START_ADV_USEFUL}""")
pbp_df['won'] = pbp_df['won'].astype(bool)

average_plays_game = pbp_df.groupby(["game_id"]).agg({'play_sequence': "count"}).mean().iat[0]
average_plays_game_factor = pbp_df.groupby(["game_id"]).agg({'play_sequence': "count"}).apply(
    lambda x: average_plays_game / x)
pbp_df['gei'] = pbp_df['team_2_wpa'].abs()
gei_df = pbp_df.groupby(["game_id"]).agg({'gei': "sum"})

gei_df = pd.concat([average_plays_game_factor, gei_df], axis=1)
gei_df['gei'] = gei_df['play_sequence'] * gei_df['gei']

gsi_df = pbp_df.groupby(["game_id"]).agg({"team_1_score": "max", "team_2_score": "max"})
gsi_df['total_score'] = gsi_df['team_1_score'] + gsi_df['team_2_score']
average_total_score_game = gsi_df['total_score'].mean()
gsi_df['gsi'] = gsi_df['total_score'] / average_total_score_game

pbp_df.loc[pbp_df['won'], "cbf"] = pbp_df['wp']
pbp_df.loc[~pbp_df['won'], "cbf"] = 1 - pbp_df['wp']

cbf_df = pbp_df.groupby(["game_id"]).agg({'cbf': "min"})
cbf_df['cbf'] = 1 / cbf_df['cbf']

games_df = pd.concat([gei_df, gsi_df, cbf_df], axis=1)
del gei_df, gsi_df, cbf_df, average_total_score_game, average_plays_game, average_plays_game_factor
games_df['gsi'] = games_df['gei'] * games_df['gsi']

games_df['gei_pct'] = games_df['gei'].rank(pct=True)
games_df['gei_rank'] = games_df['gei'].rank(ascending=False).astype(int)
games_df['gsi_pct'] = games_df['gsi'].rank(pct=True)
games_df['gsi_rank'] = games_df['gsi'].rank(ascending=False).astype(int)
games_df['cbf_pct'] = games_df['cbf'].rank(pct=True)
games_df['cbf_rank'] = games_df['cbf'].rank(ascending=False).astype(int)
games_df.reset_index(inplace=True)
games_df.drop(['play_sequence', 'team_1_score', 'team_2_score', 'total_score'], axis=1, inplace=True)
store.store_dataframe(games_df, "gei", if_exists=store.IF_EXISTS_REPLACE)

print("Done")
