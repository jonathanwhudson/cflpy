from sklearn.linear_model import LogisticRegression
import numpy as np
import pandas as pd

import store_basic

pd.set_option('display.max_colwidth', None)

YARD_LINE_OLD = 5
YARD_LINE_NEW = 25
YEAR_NEW = 2015
CONVERT = 3
KICKOFF = 4
FIELD_GOAL = 5
PUNT = 6

query = f'''
    SELECT games.year, pbp.game_id, games.event_type_id, pbp.ball_carrier_last_name, pbp.ball_carrier_first_name, pbp.play_summary, drives.distance, pbp.play_result_type_id, pbp.play_result_points
    FROM pbp , drives, games
    WHERE    pbp.play_id = drives.play_id 
    AND pbp.entry = drives.entry
    AND pbp.game_id=games.game_id
    AND pbp.play_type_id = {FIELD_GOAL}
    ORDER BY pbp.game_id;
'''

field_goals = store_basic.query(query)

field_goals.columns = ["year", "game", "event", "last", "first", "summary", "dist_from_end_zone", "result",
                                    "points"]


def categorize(row):
    # No designation
    # Some were scoring types so these are unsuccessful field goals with points scored that were field goal points (assumed)
    if row['result'] == 0 and row['points'] == 1:
        return "UNSUCCESSFUL-ROUGE"
    if row['result'] == 0 and row['points'] == 2:
        return "UNSUCCESSFUL-SAFETY"
    if row['result'] == 0 and row['points'] == 6:
        return "UNSUCCESSFUL-TD"
    # Some were clear no plays (also check summary to decide)
    if row['result'] == 0 and row['points'] == 0 and "penalty" in str(row['summary']).lower():
        return "NO-PLAY-PENALTY"
    if row['result'] == 0 and row['points'] == 0 and "no play" in str(row['summary']).lower():
        return "NO-PLAY-ACTUAL"
    # Others were unsucessful field goals that weren't categorize so decide by summary
    if row['result'] == 0 and row['points'] == 0 and "blocked field goal" in str(row['summary']).lower():
        return "BLOCKED?"
    if row['result'] == 0 and row['points'] == 0 and "missed field goal" in str(row['summary']).lower():
        return "MISSED?"
    # Others are odd like this one run play?
    if row['result'] == 0 and row['points'] == 0 and "run" in str(row['summary']).lower():
        return "RUN?"
    # Touchdown result for the team which happens likely on miss that goes weird
    if row['result'] == 1 and row['points'] == 6:
        return "MISS-TD(FOR)"
    # Field goal made (two games were points weren't given to these so we categorize those)
    if row['result'] == 2 and row['points'] == 0:
        return "MADE-(FORGOTTOGIVEPTS?)"
    if row['result'] == 2 and row['points'] == 3:
        return "MADE"
    # Turnover on downs (which is due to block or other mistake and generally not on kicker)
    if row['result'] == 4 and row['points'] == 0:
        return "DOWNS(BLOCK)"
    # Miss field goal
    if row['result'] == 5 and row['points'] == 0:
        return "MISS"
    if row['result'] == 5 and row['points'] == 1:
        return "MISS-ROUGE"
    if row['result'] == 5 and row['points'] == 6:
        return "MISS-TD(AGST)"
    # Called fumble but generally summary indicate these were all blocks
    if row['result'] == 7 and row['points'] == 0:
        return "FUMBLE(BLOCK)"
    if row['result'] == 7 and row['points'] == 6:
        return "FUMBLE(BLOCK)-TD(AGST)"
    return "UNKNOWN"


def success(row):
    # No designation
    # Some were scoring types so these are unsuccessful field goals with points scored that were field goal points (assumed)
    if row['category'] == "UNSUCCESSFUL-ROUGE":
        return "MISS"
    if row['category'] == "UNSUCCESSFUL-SAFETY":
        return "MISS"
    if row['category'] == "UNSUCCESSFUL-TD":
        return "MISS"
    # Some were clear no plays (also check summary to decide)
    if row['category'] == "NO-PLAY-PENALTY":
        return "IGNORE"
    if row['category'] == "NO-PLAY-ACTUAL":
        return "IGNORE"
    # Others were unsucessful field goals that weren't categorize so decide by summary
    if row['category'] == "BLOCKED?":
        return "BLOCK"
    if row['category'] == "MISSED?":
        return "MISS"
    # Others are odd like this one run play?
    if row['category'] == "RUN?":
        return "IGNORE"
    # Touchdown result for the team which happens likely on miss that goes weird
    if row['category'] == "MISS-TD(FOR)":
        return "MISS"
    # Field goal made (two games were points weren't given to these so we categorize those)
    if row['category'] == "MADE-(FORGOTTOGIVEPTS?)":
        return "MADE"
    if row['category'] == "MADE":
        return "MADE"
    # Turnover on downs (which is due to block or other mistake and generally not on kicker)
    if row['category'] == "DOWNS(BLOCK)":
        return "BLOCK"
    # Miss field goal
    if row['category'] == "MISS":
        return "MISS"
    if row['category'] == "MISS-ROUGE":
        return "MISS"
    if row['category'] == "MISS-TD(AGST)":
        return "MISS"
    # Called fumble but generally summary indicate these were all blocks
    if row['category'] == "FUMBLE(BLOCK)":
        return "BLOCK"
    if row['category'] == "FUMBLE(BLOCK)-TD(AGST)":
        return "BLOCK"
    return "UNKNOWN"


def points(row):
    if row['success'] == "MISS":
        return 0
    if row['success'] == "BLOCK":
        return 0
    if row['success'] == "IGNORE":
        return None
    if row['success'] == "MADE":
        return 3
    return None


field_goals['distance'] = field_goals['dist_from_end_zone'] + 7
field_goals['category'] = field_goals.apply(categorize, axis=1)
field_goals['success'] = field_goals.apply(success, axis=1)
field_goals['kicker_points'] = field_goals.apply(points, axis=1)
field_goals['made'] = np.where(field_goals['kicker_points'] > 0, 1, 0)


query = f'''
    SELECT games.year, pbp.game_id, games.event_type_id, pbp.ball_carrier_last_name, pbp.ball_carrier_first_name, pbp.play_summary, drives.distance, pbp.play_result_type_id, pbp.play_result_points
    FROM pbp , drives, games
    WHERE    pbp.play_id = drives.play_id 
    AND pbp.entry = drives.entry
    AND pbp.game_id=games.game_id
    AND pbp.play_type_id = {CONVERT}
    ORDER BY pbp.game_id;'''


converts = store_basic.query(query)

converts.columns = ["year", "game", "event", "last", "first", "summary", "dist_from_end_zone", "result",
                                 "points"]


def categorize_conv(row):
    if row['result'] == 0 and "penalty" in str(row['summary']).lower() and "declined" in str(row['summary']).lower():
        return "MISS-PENALTY-DECLINED"
    if row['result'] == 0 and "penalty" in str(row['summary']).lower():
        return "NO-PLAY"
    if row['result'] == 0:
        return "MISS"
    if row['result'] == 1 and row['points'] == 0:
        return "MISSED"
    if row['result'] == 1 and row['points'] == 2:
        return "MISSED-ACTION-POINTS"
    if row['result'] == 1 and row['points'] == 1:
        return "MADE"
    if row['result'] == 1 and row['points'] == 6:
        return "TD?"
    return "UNKNOWN"


def success_conv(row):
    if row['category'] == "MISS-PENALTY-DECLINED":
        return "MISS"
    if row['category'] == "NO-PLAY":
        return "IGNORE"
    if row['category'] == "MISS":
        return "MISS"
    if row['category'] == "MISSED":
        return "MISS"
    if row['category'] == "MISSED-ACTION-POINTS":
        return "MISS"
    if row['category'] == "MADE":
        return "MADE"
    if row['category'] == "TD?":
        return "IGNORE"
    return "UNKNOWN"


def points_conv(row):
    if row['success'] == "MISS":
        return 0
    if row['success'] == "IGNORE":
        return None
    if row['success'] == "MADE":
        return 1
    return None


converts['distance'] = np.where(converts['year'] < YEAR_NEW, YARD_LINE_OLD + 7, YARD_LINE_NEW + 7)
converts['category'] = converts.apply(categorize_conv, axis=1)
converts['success'] = converts.apply(success_conv, axis=1)
converts['kicker_points'] = converts.apply(points_conv, axis=1)
converts['made'] = np.where(converts['kicker_points'] > 0, 1, 0)

field_goals_limit = field_goals.loc[(field_goals['success'] == 'MISS') | (field_goals['success'] == 'MADE')]
field_goals_limit = field_goals_limit[field_goals_limit['event'] > 0]
converts_limit = converts.loc[(converts['success'] == 'MISS') | (converts['success'] == 'MADE')]
converts_limit = converts_limit[converts_limit['event'] > 0]

field_goals_input = field_goals_limit.loc[(field_goals_limit['year'] >= 2009) & (field_goals_limit['year'] < 2022)]
converts_input1 = converts_limit.loc[(converts_limit['year'] >= 2009) & (converts_limit['year'] < 2015)]
converts_input2 = converts_limit.loc[(converts_limit['year'] >= 2015) & (converts_limit['year'] < 2022)]

model_fg = LogisticRegression()
model_c1 = LogisticRegression()
model_c2 = LogisticRegression()
model_fg.fit(field_goals_input['distance'].values.reshape(-1, 1), field_goals_input['made'])
model_c1.fit(converts_input1['distance'].values.reshape(-1, 1), converts_input1['made'])
model_c2.fit(converts_input2['distance'].values.reshape(-1, 1), converts_input2['made'])

print(model_fg.predict_proba([[YARD_LINE_OLD + 7]]))
print(model_c1.predict_proba([[YARD_LINE_OLD + 7]]))

print(model_fg.predict_proba([[YARD_LINE_NEW + 7]]))
print(model_c2.predict_proba([[YARD_LINE_NEW + 7]]))

fg_pred = model_fg.predict_proba(field_goals['distance'].values.reshape(-1, 1))

field_goals['rate'] = pd.DataFrame(fg_pred, columns=["MISS", "MADE"])['MADE']
field_goals['rate_above'] = field_goals['made'] - field_goals['rate']
field_goals['points_expected'] = field_goals['rate'] * 3
field_goals['points_above'] = field_goals['kicker_points'] - field_goals['points_expected']

field_goals_filter = field_goals.loc[
    (field_goals['event'] >= 1) & (field_goals['event'] <= 3) & (field_goals['year'] >= 2022) & (
                field_goals['year'] <= 2022)]

count = field_goals_filter.groupby(['last', 'first'])['last'].count()
average_distance = field_goals_filter.groupby(['last', 'first'])['distance'].mean()
actual_perc = field_goals_filter.groupby(['last', 'first'])['made'].mean()
expected_perc = field_goals_filter.groupby(['last', 'first'])['rate'].mean()
rate_above = field_goals_filter.groupby(['last', 'first'])['rate_above'].mean()
actual_pts = field_goals_filter.groupby(['last', 'first'])['kicker_points'].sum()
expected_pts = field_goals_filter.groupby(['last', 'first'])['points_expected'].sum()
points_above = field_goals_filter.groupby(['last', 'first'])['points_above'].sum()

result = pd.concat(
    [count, average_distance, actual_perc, expected_perc, rate_above, actual_pts, expected_pts, points_above], axis=1)
result.columns = ['count', 'average_distance', 'actual_fg%', "expected_fg%", "fg%_above_exp", "actual_pts",
                  "expected_pts", "pts_above_exp"]
result['avg_pts_above_exp'] =result['pts_above_exp']/result['count']

sorted_fg_pts = result.sort_values(by=["pts_above_exp", "fg%_above_exp", "last", "first"], ascending=False)
#sorted_fg_pts = sorted_fg_pts[sorted_fg_pts['count'] >= 25]
print(sorted_fg_pts[:25].to_string())
sorted_fg_perc = result.sort_values(by=["fg%_above_exp", "pts_above_exp", "last", "first"], ascending=False)
#sorted_fg_perc = sorted_fg_perc[sorted_fg_perc['count'] >= 25]
print(sorted_fg_perc[:25].to_string())

c1_pred = model_c1.predict_proba(converts['distance'].values.reshape(-1, 1))
c2_pred = model_c2.predict_proba(converts['distance'].values.reshape(-1, 1))

converts['rate_old'] = pd.DataFrame(c1_pred, columns=["MISS", "MADE"])['MADE']
converts['rate_new'] = pd.DataFrame(c2_pred, columns=["MISS", "MADE"])['MADE']

converts['rate'] = np.where(converts['year'] < 2015, converts['rate_old'], converts['rate_new'])
del converts['rate_old']
del converts['rate_new']
converts['rate_above'] = converts['made'] - converts['rate']
converts['points_expected'] = converts['rate'] * 1
converts['points_above'] = converts['kicker_points'] - converts['points_expected']

converts_filter = converts.loc[
    (converts['event'] >= 1) & (converts['event'] <= 3) & (converts['year'] >= 2022) & (converts['year'] <= 2022)]

count = converts_filter.groupby(['last', 'first'])['last'].count()
average_distance = converts_filter.groupby(['last', 'first'])['distance'].mean()
actual_perc = converts_filter.groupby(['last', 'first'])['made'].mean()
expected_perc = converts_filter.groupby(['last', 'first'])['rate'].mean()
rate_above = converts_filter.groupby(['last', 'first'])['rate_above'].mean()
actual_pts = converts_filter.groupby(['last', 'first'])['kicker_points'].sum()
expected_pts = converts_filter.groupby(['last', 'first'])['points_expected'].sum()
points_above = converts_filter.groupby(['last', 'first'])['points_above'].sum()

result = pd.concat(
    [count, average_distance, actual_perc, expected_perc, rate_above, actual_pts, expected_pts, points_above], axis=1)
result.columns = ['count', 'average_distance', 'actual_xp%', "expected_xp%", "xp%_above_exp", "actual_pts",
                  "expected_pts", "pts_above_exp"]
result['avg_pts_above_exp'] =result['pts_above_exp']/result['count']

sorted_conv_pts = result.sort_values(by=["pts_above_exp", "xp%_above_exp", "last", "first"], ascending=False)
# %sorted_conv_pts = sorted_conv_pts[sorted_conv_pts['count'] >= 25]
print(sorted_conv_pts[:25].to_string())
sorted_conv_perc = result.sort_values(by=["xp%_above_exp", "pts_above_exp", "last", "first"], ascending=False)
# sorted_conv_perc = sorted_conv_perc[sorted_conv_perc['count'] >= 25]
print(sorted_conv_perc[:25].to_string())

sorted_fg_pts['average_distance'] = sorted_fg_pts['average_distance'].round(decimals=2)
sorted_fg_pts['actual_fg%'] = sorted_fg_pts['actual_fg%'] * 100
sorted_fg_pts['actual_fg%'] = sorted_fg_pts['actual_fg%'].round(decimals=2)
sorted_fg_pts['expected_fg%'] = sorted_fg_pts['expected_fg%'] * 100
sorted_fg_pts['expected_fg%'] = sorted_fg_pts['expected_fg%'].round(decimals=2)
sorted_fg_pts['fg%_above_exp'] = sorted_fg_pts['fg%_above_exp'] * 100
sorted_fg_pts['fg%_above_exp'] = sorted_fg_pts['fg%_above_exp'].round(decimals=2)
sorted_fg_pts['actual_pts'] = sorted_fg_pts['actual_pts'].astype(int)
sorted_fg_pts['expected_pts'] = sorted_fg_pts['expected_pts'].round(decimals=2)
sorted_fg_pts['pts_above_exp'] = sorted_fg_pts['pts_above_exp'].round(decimals=2)
sorted_fg_pts = sorted_fg_pts.reset_index()
import matplotlib.pyplot as plt

fig, ax = plt.subplots()
fig.patch.set_visible(False)
ax.axis('off')
ax.axis('tight')
table = ax.table(cellText=sorted_fg_pts.values, colLabels=sorted_fg_pts.columns, loc='center')
table.auto_set_font_size(False)
table.set_fontsize(5)
fig.tight_layout()
#plt.show()
