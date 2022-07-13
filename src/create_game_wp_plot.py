import datetime

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

import store_basic

cfl_colors = {}
cfl_colors["BC"] = "#FC4C02"
cfl_colors["EDM"] = "#2C5234"
cfl_colors["HAM"] = "#FFB81C"
cfl_colors["OTT"] = "#000000"
cfl_colors["SSK"] = "#006341"
cfl_colors["TOR"] = "#5F8FB4"
cfl_colors["WPG"] = "#003087"
cfl_colors["MTL"] = "#8C2334"
cfl_colors["CGY"] = "#C8102E"

GAME_ID = 6229

def main() -> None:
    plot = create_plot(GAME_ID)
    plot.show()
    plot.close()


def get_plays_to_plot(game_id: int) -> pd.DataFrame:
    df = store_basic.query(f'''
        SELECT pbp.play_id,pbp.play_summary,drives.points_scored,drives.quarter,epa.team_1_wp,epa.team_2_wp,drives.time_remaining,drives.won,drives.home,pbp.team_id,pbp.play_result_points,pbp.play_result_type_id,pbp.play_success_id,pbp.play_type_id FROM pbp LEFT JOIN epa ON pbp.play_id=epa.play_id AND pbp.entry=epa.entry LEFT JOIN drives ON pbp.play_id=drives.play_id AND pbp.entry=drives.entry WHERE pbp.game_id={game_id} ORDER BY pbp.play_sequence, pbp.entry
    ''')
    prev_tr = None
    for index, row in df.iterrows():
        if prev_tr is not None and row['time_remaining'] == prev_tr:
            df.at[index, 'time_remaining'] = row['time_remaining'] + 0.5
        prev_tr = row['time_remaining']
    count = 1
    for index, row in df.iterrows():
        if row['quarter'] > 4:
            df.at[index, 'time_remaining'] = count * -22.5
            count += 1
    df = pd.concat([df,pd.DataFrame(df.tail(1))], ignore_index=True)
    if df['home'].iat[-1] == 1:
        df['team_2_wp'].iat[-1] = df['won'].iat[-1]
        df['team_1_wp'].iat[-1] = 1-df['won'].iat[-1]
    if df['home'].iat[-1] == 0:
        df['team_1_wp'].iat[-1] = df['won'].iat[-1]
        df['team_2_wp'].iat[-1] = 1-df['won'].iat[-1]
    df['play_result_type_id'].iat[-1] = 0
    df['play_success_id'].iat[-1] = 0
    df['play_type_id'].iat[-1] = 0
    df['play_result_points'].iat[-1] = 0
    df = df.dropna()
    df["home_wp"] = df["team_2_wp"]
    df["away_wp"] = df["team_1_wp"]
    df["game_seconds_remaining"] = max(df["time_remaining"]) - df["time_remaining"]
    return df


def create_plot(game_id: int) -> matplotlib.pyplot:
    game_df = get_plays_to_plot(game_id)

    # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
    #   print(game_df)

    home_color, away_color = get_home_away_colors(game_id)
    max_quarter = get_max_quarter(game_id)
    ot = max_quarter > 4
    title, subtitle = get_titles(game_id)
    gei_string = get_gei_string(game_id)
    gsi_string = get_gsi_string(game_id)
    comeback_string = get_comeback_string(game_id)

    # Create a figure
    fig, ax = plt.subplots(figsize=(16, 8))
    ax.set_xlim(left=0)
    plt.ylim(0, 1)

    # Generate line plots
    sns.lineplot(x='game_seconds_remaining', y='away_wp', data=game_df, color=f'{away_color}', linewidth=2)
    sns.lineplot(x='game_seconds_remaining', y='home_wp', data=game_df, color=f'{home_color}', linewidth=2)

    # Generate fill
    ax.fill_between(game_df['game_seconds_remaining'], 0.5, game_df['away_wp'], where=game_df['home'] <= 0,
                    color=f'{away_color}', alpha=0.3, interpolate=True)
    ax.fill_between(game_df['game_seconds_remaining'], 0.5, game_df['home_wp'], where=game_df['home'] > 0,
                    color=f'{home_color}', alpha=0.3, interpolate=True)

    # Labels
    plt.ylabel('Win Probability', fontsize=16)
    plt.xlabel('', fontsize=16)

    # X
    x_ticks_labels = ['Kickoff', 'End\nQ1', 'Half', 'End\nQ3', 'End']
    x_ticks_positions = [0, 900, 1800, 2700, 3600]
    if ot:
        x_ticks_labels[-1] = "End\nReg"
        x_ticks_labels.append("End\nOT")
        x_ticks_positions.append(game_df['game_seconds_remaining'].max())
    ax.set_xticks(x_ticks_positions)
    ax.set_xticklabels(x_ticks_labels, fontsize=12)

    # Y
    ax.set_yticks([0, 0.25, 0.5, 0.75, 1])
    ax.set_yticklabels(["", "25%", "50%", "75%", "100%"], fontsize=12)

    # Grid
    for x_pos in x_ticks_positions[1:-1]:
        plt.axvline(x=x_pos, color='black', alpha=0.7)
    plt.axhline(y=.25, color='black', alpha=0.3)
    plt.axhline(y=.50, color='black', alpha=0.7)
    plt.axhline(y=.75, color='black', alpha=0.3)

    # Titles
    plt.suptitle(title, fontsize=20, style='italic', weight='bold')
    plt.title(subtitle, fontsize=16, style='italic', weight='semibold')

    # Creating a textbox with GEI score
    props = dict(boxstyle='round', facecolor='white', alpha=0.6)
    plt.figtext(.133, .83, gei_string + "\n" + gsi_string + "\n" + comeback_string, style='italic', bbox=props,
                fontsize=8)

    # Citations
    plt.figtext(0.131, 0.120, 'Graph: @jonathanwhudson | Data: @cfl', fontsize=8)
    for i, row in game_df.iterrows():
        label = ""
        if row['play_success_id'] == 3:
            label = "XP"
        if row['play_success_id'] == 6:
            label = "2PT"
        if row['play_success_id'] == 10:
            label = "FG"
        if row['play_success_id'] == 11:
            label = "FG FAIL"
        if row['play_success_id'] == 13:
            label = "ROUGE"
        if row['play_success_id'] == 16:
            label = "(miss-fg)TD"
        if row['play_success_id'] == 24:
            label = "FUMBLE RETURN TD"
        if row['play_success_id'] == 29:
            label = ""
        if row['play_success_id'] == 47:
            label = ""
        if row['play_success_id'] == 58:
            label = "punt"
        if row['play_success_id'] == 74:
            label = ""
        if row['play_success_id'] == 78:
            label = ""
        if row['play_success_id'] == 98:
            label = ""
        if row['play_success_id'] == 110:
            label = "SAFETY"
        if row['play_success_id'] == 111:
            label = "(no-xp)"
        if row['play_success_id'] == 116:
            label = "FAILED 1PT2"
        if row['play_success_id'] == 900:
            label = ""
        if row['play_success_id'] == 901:
            label = "(no-xp)"
        if row['play_success_id'] == 902:
            label = "(no-2pt)"
        if row['play_success_id'] == 903:
            label = "(no-2pt)"
        if row['play_result_type_id'] == 7:
            label += "(fumble)"
        if row['play_result_type_id'] == 6:
            label += "(int)"
        if row['play_result_type_id'] == 5:
            label += "(miss-fg)"
        if row['play_result_type_id'] == 4:
            label += "(downs)"
        if row['play_result_type_id'] == 3:
            label += "punt"
        if row['play_result_type_id'] == 2:
            label += "FG"
        if row['play_result_type_id'] == 1 and row['play_type_id'] not in [0,3,8,9] :
            label += "TD"
        if abs(row['points_scored']) != 0:
            label += "("+str(int(row['points_scored']))+")"

        label = label.replace("puntpunt","punt")
        label = label.replace("TD(6)", "TD")
        label = label.replace("XPTD", "XP")
        label = label.replace("2PTTD", "2PT")
        label = label.replace("(no-xp)TD", "(no-xp)")
        label = label.replace("(no-2pt)TD", "(no-2pt)")
        label = label.replace("XP(1)", "XP")
        label = label.replace("2PT(2)", "2PT")
        label = label.replace("FGFG", "FG")
        label = label.replace("FG(3)","FG")
        label = label.replace("SAFETY(-2)","SAFETY")
        label = label.replace("(-6)","TD")
        label = label.replace("ROUGE(miss-fg)","(miss-fg)\nROUGE")
        label = label.replace("ROUGE(1)", "ROUGE")
        label = label.replace("(fumble)TD","(fumble)\nTD")
        label = label.replace("punt(1)", "punt\nROUGE")
        label = label.replace("(int)TD","(int)\nTD")
        label = label.replace("(miss-fg)TD(miss-fg)TD","(miss-fg)\nTD")

        offset = 0.05
        if label != "":
            if row['home']:
                if row['home_wp'] > 0.5:
                    if row['points_scored'] < 0:
                        plt.text(row['game_seconds_remaining'], row['home_wp'] + offset, label, color=f'{away_color}',
                                 ha='center', va="top")
                    else:
                        plt.text(row['game_seconds_remaining'], row['home_wp'] + offset, label, color=f'{home_color}',
                                 ha='center', va="top")
                else:
                    if row['points_scored'] < 0:
                        plt.text(row['game_seconds_remaining'], row['home_wp'] - offset, label, color=f'{away_color}',
                                 ha='center', va="bottom")
                    else:
                        plt.text(row['game_seconds_remaining'], row['home_wp'] - offset, label, color=f'{home_color}',
                                 ha='center', va="bottom")
            else:
                if row['away_wp'] > 0.5:
                    if row['points_scored'] < 0:
                        plt.text(row['game_seconds_remaining'], row['away_wp'] + offset, label, color=f'{home_color}',
                                 ha='center', va="top")
                    else:
                        plt.text(row['game_seconds_remaining'], row['away_wp'] + offset, label, color=f'{away_color}',
                                 ha='center', va="top")
                else:
                    if row['points_scored'] < 0:
                        plt.text(row['game_seconds_remaining'], row['away_wp'] - offset, label, color=f'{home_color}',
                                 ha='center', va="bottom")
                    else:
                        plt.text(row['game_seconds_remaining'], row['away_wp'] - offset, label, color=f'{away_color}',
                                 ha='center', va="bottom")
    return plt


def get_home_away_colors(game_id: int) -> (str, str):
    home_team, away_team = get_home_away_teams(game_id)
    home_abbr = get_team_abbr(game_id,home_team)
    away_abbr = get_team_abbr(game_id,away_team)
    return cfl_colors[home_abbr], cfl_colors[away_abbr]


def get_home_away_teams(game_id: int) -> (int, int):
    df = store_basic.query(f"SELECT team_1_team_id, team_2_team_id FROM games WHERE game_id={game_id};")
    return df['team_2_team_id'].iat[0], df['team_1_team_id'].iat[0]


def get_home_away_scores(game_id: int) -> (int, int):
    df = store_basic.query(f"SELECT team_1_score, team_2_score FROM games WHERE game_id={game_id};")
    return df['team_2_score'].iat[0], df['team_1_score'].iat[0]


def get_team_abbr(game_id: int, team_id: int) -> str:
    df = store_basic.query(
        f"SELECT team_1_team_id, team_2_team_id, team_1_abbreviation, team_2_abbreviation FROM games WHERE game_id={game_id};")
    if df['team_1_team_id'].iat[0] == team_id:
        return df['team_1_abbreviation'].iat[0]
    else:
        return df['team_2_abbreviation'].iat[0]


def get_team_full_name(game_id: int, team_id: int) -> str:
    df = store_basic.query(
        f"SELECT team_1_team_id, team_2_team_id, team_1_nickname, team_2_nickname FROM games WHERE game_id={game_id};")
    if df['team_1_id'].iat[0] == team_id:
        return df['team_1_nickname'].iat[0]
    else:
        return df['team_2_nickname'].iat[0]


def get_venue_name(game_id: int) -> str:
    df = store_basic.query(f"SELECT venue FROM games WHERE game_id={game_id}")
    return df['venue'].iat[0]


def get_event_type(game_id: int) -> str:
    df = store_basic.query(f"SELECT event_type FROM games WHERE game_id={game_id}")
    return df['event_type'].iat[0]


def get_max_quarter(game_id: int) -> int:
    df = store_basic.query(f"SELECT quarter FROM pbp WHERE game_id={game_id}")
    return df['quarter'].max()


def get_year_week_date(game_id: int) -> (int, int, datetime):
    df = store_basic.query(f"SELECT year, week, date_start FROM games WHERE game_id={game_id}")
    return df['year'].iat[0], df['week'].iat[0], df['date_start'].iat[0]


def get_titles(game_id: int) -> (str, str):
    home_team, away_team = get_home_away_teams(game_id)
    home_score, away_score = get_home_away_scores(game_id)
    home_abbr = get_team_abbr(game_id, home_team)
    away_abbr = get_team_abbr(game_id, away_team)
    # home_full_name = get_team_full_name(home_team)
    # away_full_name = get_team_full_name(away_team)
    venue_name = get_venue_name(game_id)
    event_type_name = get_event_type(game_id)
    year, week, date_start = get_year_week_date(game_id)
    max_quarter = get_max_quarter(game_id)
    ot = max_quarter > 4
    ot_str = " OT" if ot else ""
    title = f'{event_type_name} {year} Week {week} - {away_abbr} {away_score} @ {home_abbr} {home_score}{ot_str}'
    subtitle = f'Venue: {venue_name} Date: {date_start} EST'
    return title, subtitle


def get_gei(game_id: int) -> float:
    df = store_basic.query(f"SELECT gei,gei_pct FROM gei WHERE game_id={game_id}")
    return df['gei'].iat[0], df['gei_pct'].iat[0]


def get_gsi(game_id: int) -> float:
    df = store_basic.query(f"SELECT gsi,gsi_pct FROM gei WHERE game_id={game_id}")
    return df['gsi'].iat[0], df['gsi_pct'].iat[0]


def get_comeback(game_id: int) -> float:
    df = store_basic.query(f"SELECT cbf,cbf_pct FROM gei WHERE game_id={game_id}")
    return df['cbf'].iat[0], df['cbf_pct'].iat[0]


def get_gei_string(game_id: int) -> str:
    gei, perc = get_gei(game_id)
    return f'Game Excitement Index (GEI): {gei:2.2f} - {perc * 100:3.1f}%'


def get_gsi_string(game_id: int) -> str:
    gsi, perc = get_gsi(game_id)
    return f'Game Shootout Index (GSI): {gsi:2.2f} - {perc * 100:3.1f}%'


def get_comeback_string(game_id: int) -> str:
    cbf, perc = get_comeback(game_id)
    return f'Comeback Factor (CBF): {cbf:2.2f} - {perc * 100:3.1f}%'


if __name__ == "__main__":
    main()
