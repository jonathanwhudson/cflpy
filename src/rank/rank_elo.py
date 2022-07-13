from store import store_helper


def main() -> None:
    store = model(2022, 38)
    for x, y in sorted(store.items(), key=lambda x: x[1], reverse=True):
        print(f"{x}\t{y * 100:.2f}%")

def model(year, games_played) -> None:
    names = ["BC", "CGY", "EDM", "HAM", "MTL", "OTT", "SSK", "TOR", "WPG"]
    zipped = getTeam(year, names)
    result = []
    for x in zipped:
        result.append((x[0], ((x[1] - 1500) / (20 * (games_played)/9)) / 2 + 0.5))
    store = {}
    for x,y in result:
        store[x] = y
    return store


def getTeam(year: int, names: list[str]) -> list[(str, float)]:
    store = {}
    for i in range(1, len(names) + 1):
        query = f'''
            SELECT games.team_1_team_id, games.team_2_team_id, team_1_elo_season_out, team_2_elo_season_out
            FROM games, elo
            WHERE games.game_id = elo.game_id
            AND games.year = {year}        
            AND event_status_id = 4
            AND (games.team_1_team_id = {i} OR games.team_2_team_id = {i})
            ORDER BY games.date_start DESC
            LIMIT 1
        '''
        data = store_helper.load_dataframe_from_query(query)
        if i == data["team_1_team_id"].iat[0]:
            store[i] = data["team_1_elo_season_out"].iat[0]
        else:
            store[i] = data["team_2_elo_season_out"].iat[0]
    result = []
    for i in range(len(names)):
        result.append((names[i], store[i + 1]))
    return result


if __name__ == "__main__":
    main()
