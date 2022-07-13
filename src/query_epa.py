
#def main():
import store_basic

epa_df = store_basic.query(
    f"""SELECT epa.*,pbp.*,games.* FROM epa LEFT JOIN pbp ON pbp.play_id=epa.play_id AND pbp.entry=epa.entry LEFT JOIN games on pbp.game_id=games.game_id WHERE epa.entry in (0,1) AND epa.YEAR=2022 AND games.event_type_id > 0""")
epa_df.loc[epa_df["team_id"] == epa_df["team_1_team_id"], 'd_team_id'] = epa_df["team_2_abbreviation"]
epa_df.loc[epa_df["team_id"] == epa_df["team_2_team_id"], 'd_team_id'] = epa_df["team_1_abbreviation"]
fantasy_df = store_basic.query("SELECT * FROM fantasy")
fantasy_df['temp1'] = fantasy_df['First'].str.lower()
fantasy_df['temp2'] = fantasy_df['Last'].str.lower()
fantasy_df['Cost'] = fantasy_df['Cost'].astype(int)
ball_carriers_df = epa_df[['ball_carrier_cfl_central_id','ball_carrier_first_name','ball_carrier_last_name']].drop_duplicates()
ball_carriers_gp_df = epa_df[['ball_carrier_cfl_central_id','game_id']].drop_duplicates().groupby('ball_carrier_cfl_central_id').agg({'ball_carrier_cfl_central_id':['count']}).reset_index()
ball_carriers_gp_df.columns = ['ball_carrier_cfl_central_id','gp']
ball_carriers_df = ball_carriers_df.merge(ball_carriers_gp_df, on="ball_carrier_cfl_central_id")
ball_carriers_df['temp1'] = ball_carriers_df['ball_carrier_first_name'].str.lower()
ball_carriers_df['temp2'] = ball_carriers_df['ball_carrier_last_name'].str.lower()
ball_carriers_df = ball_carriers_df.merge(fantasy_df, on=["temp1", "temp2"], how="left")

quarterbacks_df = epa_df[['quarterback_cfl_central_id','quarterback_first_name','quarterback_last_name']].drop_duplicates()
quarterbacks_gp_df = epa_df[['quarterback_cfl_central_id','game_id']].drop_duplicates().groupby('quarterback_cfl_central_id').agg({'quarterback_cfl_central_id':['count']}).reset_index()
quarterbacks_gp_df.columns = ['quarterback_cfl_central_id','gp']
quarterbacks_df = quarterbacks_df.merge(quarterbacks_gp_df, on="quarterback_cfl_central_id")
quarterbacks_df['temp1'] = quarterbacks_df['quarterback_first_name'].str.lower()
quarterbacks_df['temp2'] = quarterbacks_df['quarterback_last_name'].str.lower()
quarterbacks_df = quarterbacks_df.merge(fantasy_df, on=["temp1", "temp2"], how="left")

primary_defenders_df = epa_df[['primary_defender_cfl_central_id','primary_defender_first_name','primary_defender_last_name']].drop_duplicates()
primary_defenders_gp_df = epa_df[['primary_defender_cfl_central_id','game_id']].drop_duplicates().groupby('primary_defender_cfl_central_id').agg({'primary_defender_cfl_central_id':['count']}).reset_index()
primary_defenders_gp_df.columns = ['primary_defender_cfl_central_id','gp']
primary_defenders_df = primary_defenders_df.merge(primary_defenders_gp_df, on="primary_defender_cfl_central_id")
primary_defenders_df['temp1'] = primary_defenders_df['primary_defender_first_name'].str.lower()
primary_defenders_df['temp2'] = primary_defenders_df['primary_defender_last_name'].str.lower()
primary_defenders_df = primary_defenders_df.merge(fantasy_df, on=["temp1", "temp2"], how="left")


bc_r = epa_df.loc[(epa_df['play_type_id'] == 1) | (epa_df['play_type_id'] == 2)].groupby(["ball_carrier_cfl_central_id"]).agg({'epa':['sum','mean']})
bc_r.columns = bc_r.columns.droplevel(0)
bc_r= bc_r.reset_index()
bc_r.columns = ['ball_carrier_cfl_central_id','total','per_play']
bc_r = bc_r.merge(ball_carriers_df, on="ball_carrier_cfl_central_id", how="left")
bc_r['per_game'] = bc_r['total']/bc_r['gp']
bc_r['epa_per_dollar'] = bc_r['total']/bc_r['Cost']
bc_r['epa_play_per_dollar'] = bc_r['per_play']/bc_r['Cost']
bc_r['epa_game_per_dollar'] = bc_r['per_game']/bc_r['Cost']
bc_r['Pos'] = bc_r['Pos'].fillna("")
RB = bc_r.loc[(bc_r['Pos'] == "RB") | (bc_r['Pos'] == "")]
WR = bc_r.loc[(bc_r['Pos'] == "WR") | (bc_r['Pos'] == "")]

qb_r = epa_df.loc[(epa_df['play_type_id'] == 1) | (epa_df['play_type_id'] == 2)].groupby(["quarterback_cfl_central_id"]).agg({'epa':['sum', 'mean']})
qb_r.columns = qb_r.columns.droplevel(0)
qb_r= qb_r.reset_index()
qb_r.columns = ['quarterback_cfl_central_id', 'total', 'per_play']
qb_r = qb_r.merge(quarterbacks_df, on="quarterback_cfl_central_id", how="left")
qb_r['per_game'] = qb_r['total'] / qb_r['gp']
qb_r['epa_per_dollar'] = qb_r['total']/qb_r['Cost']
qb_r['epa_play_per_dollar'] = qb_r['per_play']/qb_r['Cost']
qb_r['epa_game_per_dollar'] = qb_r['per_game']/qb_r['Cost']
qb_r['Pos'] = qb_r['Pos'].fillna("")
QB = qb_r.loc[(qb_r['Pos'] == "QB") | (qb_r['Pos'] == "")]


pd_r = epa_df.loc[(epa_df['play_type_id'] == 1) | (epa_df['play_type_id'] == 2)].groupby(["d_team_id"]).agg({'epa':['sum', 'mean']})
pd_r.columns = pd_r.columns.droplevel(0)
pd_r= pd_r.reset_index()
pd_r.columns = ['d_team_id', 'total', 'per_play']

#main()
