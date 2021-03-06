import sqlalchemy as sa


def convert_to_sql_types(map_column_datatype: dict[str, str]) -> dict:
    """
    Could eventually be replaced with a mapping but for now a looping creation of datatype dictionary changing pandas dtypes to sqlalchemy dtypes
    :param map_column_datatype: The panda dictionary mapping of columns to dtypes (str,str)
    :return: New dictionary but mapping is now to sql dtypes
    """
    if type(map_column_datatype) != dict:
        raise ValueError(f"Type of columns_dtypes <{type(map_column_datatype)}> should be dict[str,str]!")
    conversion = {}
    for (column, datatype) in map_column_datatype.items():
        if datatype == "string":
            conversion[column] = sa.types.Text
        elif datatype == "int":
            conversion[column] = sa.types.Integer
        elif datatype == "Int64":
            conversion[column] = sa.types.BigInteger
        elif datatype == "datetime64[ns]":
            conversion[column] = sa.types.DateTime
        elif datatype == "datetime64[ns, UTC]":
            conversion[column] = sa.types.DateTime
        elif datatype == "float":
            conversion[column] = sa.types.Float
        elif datatype == "bool":
            conversion[column] = sa.types.Boolean
        elif datatype == "boolean":
            conversion[column] = sa.types.Boolean
        else:
            raise ValueError(f"Undefined conversion datatype <{datatype}>!")
    return conversion


VENUES = {'venue_id': 'int', 'venue_name': 'string', 'venue_capacity': 'Int64'}
SEASONS = {'season': 'int', 'preseason_start': 'datetime64[ns]', 'preseason_end': 'datetime64[ns]',
           'regular_season_start': 'datetime64[ns]', 'regular_season_end': 'datetime64[ns]',
           'semifinals_start': 'datetime64[ns]', 'semifinals_end': 'datetime64[ns]', 'finals_start': 'datetime64[ns]',
           'finals_end': 'datetime64[ns]', 'grey_cup_start': 'datetime64[ns]', 'grey_cup_end': 'datetime64[ns]'}
TEAMS = {'team_id': 'int', 'letter': 'string', 'abbreviation': 'string', 'location': 'string', 'nickname': 'string',
         'full_name': 'string', 'venue_id': 'int', 'venue_name': 'string', 'venue_capacity': 'Int64',
         'division_id': 'int', 'division_name': 'string', 'logo_image_url': 'string', 'tablet_image_url': 'string',
         'mobile_image_url': 'string', 'gametracker_small_image_url': 'string', 'gametracker_large_image_url': 'string'}
STANDINGS = {'season': 'int', 'division_id': 'int', 'division_name': 'string', 'place': 'int', 'flags': 'string',
             'team_id': 'int', 'letter': 'string', 'abbreviation': 'string', 'location': 'string', 'nickname': 'string',
             'full_name': 'string', 'games_played': 'int', 'wins': 'int', 'losses': 'int', 'ties': 'int',
             'points': 'int', 'winning_percentage': 'int', 'points_for': 'int', 'points_against': 'int',
             'home_wins': 'int', 'home_losses': 'int', 'home_ties': 'int', 'away_wins': 'int', 'away_losses': 'int',
             'away_ties': 'int', 'division_wins': 'int', 'division_losses': 'int', 'division_ties': 'int'}
CROSSOVERS = {'season': 'int', 'crossover_division_id': 'int', 'crossover_division_name': 'string',
              'crossover_place': 'int', 'division_id': 'int', 'division_name': 'string', 'place': 'int',
              'flags': 'string', 'team_id': 'int', 'letter': 'string', 'abbreviation': 'string', 'location': 'string',
              'nickname': 'string', 'full_name': 'string', 'games_played': 'int', 'wins': 'int', 'losses': 'int',
              'ties': 'int', 'points': 'int', 'winning_percentage': 'int', 'points_for': 'int',
              'points_against': 'int', 'home_wins': 'int', 'home_losses': 'int', 'home_ties': 'int', 'away_wins': 'int',
              'away_losses': 'int', 'away_ties': 'int', 'division_wins': 'int', 'division_losses': 'int',
              'division_ties': 'int'}
TRANSACTIONS = {'transaction_id': 'int', 'transaction_date': 'datetime64[ns]', 'cfl_central_id': 'int',
                'first_name': 'string', 'last_name': 'string', 'old_status_id': 'string',
                'old_status_description': 'string', 'new_status_id': 'string', 'new_status_description': 'string',
                'action_id': 'int', 'action_abbreviation': 'string', 'action_description': 'string',
                'action_additional_text': 'string', 'from_team_abbreviation': 'string',
                'to_team_abbreviation': 'string', 'position_abbreviation': 'string', 'school_name': 'string',
                'foreign_player': 'bool'}
PLAYERS = {'cfl_central_id': 'int', 'stats_inc_id': 'int', 'first_name': 'string', 'middle_name': 'string',
           'last_name': 'string', 'birth_date': 'datetime64[ns]', 'birth_place': 'string', 'height': 'float',
           'weight': 'int', 'rookie_year': 'Int64', 'foreign_player': 'bool', 'image_url': 'string', 'school_id': 'int',
           'name': 'string', 'position_id': 'int', 'offence_defence_or_special': 'string', 'abbreviation': 'string',
           'description': 'bool'}
GAMES = {'year': 'int', 'game_id': 'int', 'season': 'int', 'week': 'int', 'game_number': 'int',
         'date_start': 'datetime64[ns, UTC]', 'game_duration': 'Int64', 'attendance': 'Int64',
         'event_type_id': 'int', 'event_type': 'string', 'event_title': 'string', 'venue_id': 'int',
         'venue': 'string', 'event_status_id': 'int', 'event_status': 'string', 'is_active': 'bool',
         'quarter': 'Int64', 'minutes': 'Int64', 'seconds': 'Int64', 'down': 'Int64', 'yards_to_go': 'Int64',
         'team_1_team_id': 'int', 'team_1_location': 'string', 'team_1_nickname': 'string',
         'team_1_abbreviation': 'string', 'team_1_score': 'int', 'team_1_venue_id': 'int',
         'team_1_is_at_home': 'boolean', 'team_1_is_winner': 'boolean', 'team_2_team_id': 'int',
         'team_2_location': 'string', 'team_2_nickname': 'string', 'team_2_abbreviation': 'string',
         'team_2_score': 'int', 'team_2_venue_id': 'int', 'team_2_is_at_home': 'boolean',
         'team_2_is_winner': 'boolean', 'team_1_q1': 'Int64', 'team_1_q2': 'Int64', 'team_1_q3': 'Int64',
         'team_1_q4': 'Int64', 'team_1_ot': 'Int64', 'team_2_q1': 'Int64', 'team_2_q2': 'Int64',
         'team_2_q3': 'Int64', 'team_2_q4': 'Int64',
         'team_2_ot': 'Int64', 'coin_toss_winner': 'string',
         'coin_toss_winner_election': 'string', 'temperature': 'int', 'sky': 'string',
         'wind_speed': 'string', 'wind_direction': 'string', 'field_conditions': 'string',
         'tickets_url': 'string'}
ADV_PBP = {'game_id': 'int', 'play_id': 'int', 'play_sequence': 'int', 'quarter': 'int',
           'play_clock_start': 'string', 'play_clock_start_in_secs': 'Int64',
           'field_position_start': 'string', 'field_position_end': 'string', 'down': 'Int64',
           'yards_to_go': 'Int64', 'is_in_red_zone': 'boolean', 'team_home_score': 'int',
           'team_visitor_score': 'int', 'play_type_id': 'int',
           'play_type_description': 'string',
           'play_result_type_id': 'int', 'play_result_type_description': 'string',
           'play_result_yards': 'int', 'play_result_points': 'int',
           'play_success_id': 'int',
           'play_success_description': 'string',
           'play_change_of_possession_occurred': 'bool',
           'team_abbreviation': 'string', 'team_id': 'int', 'play_summary': 'string',
           'quarterback_cfl_central_id': 'int', 'ball_carrier_cfl_central_id': 'int',
           'primary_defender_cfl_central_id': 'int',
           'quarterback_first_name': 'string', 'ball_carrier_first_name': 'string',
           'primary_defender_first_name': 'string', 'quarterback_middle_name': 'string',
           'ball_carrier_middle_name': 'string', 'primary_defender_middle_name': 'string',
           'quarterback_last_name': 'string', 'ball_carrier_last_name': 'string',
           'primary_defender_last_name': 'string',
           'quarterback_birth_date': 'datetime64[ns]',
           'ball_carrier_birth_date': 'datetime64[ns]',
           'primary_defender_birth_date': 'datetime64[ns]', "entry": "int"}
ADV_PEN = {'game_id': 'int', 'play_id': 'int', 'play_sequence': 'int', 'quarter': 'int',
           'play_clock_start': 'string', 'play_clock_start_in_secs': 'int',
           'play_summary': 'string', 'field_position_start': 'string',
           'field_position_end': 'string', 'down': 'int', 'yards_to_go': 'int',
           'penalty_id': 'int', 'penalty_code': 'string', 'penalty_name': 'string',
           'penalty_type_id': 'int', 'penalty_type_name': 'string',
           'penalty_situation_id': 'int',
           'penalty_situation_code': 'string', 'penalty_situation_name': 'string',
           'is_major': 'int', 'was_accepted': 'int', 'team_or_player_penalty': 'string',
           'team_abbreviation': 'string', 'team_id': 'int', 'cfl_central_id': 'Int64',
           'first_name': 'string', 'middle_name': 'string', 'last_name': 'string'}
ADV_REV = {'game_id': 'int', 'play_id': 'int', 'quarter': 'int', 'play_clock_start': 'string',
           'play_clock_start_in_secs': 'int', 'play_summary': 'string',
           'field_position_start': 'string', 'field_position_end': 'string', 'down': 'int',
           'yards_to_go': 'int', 'play_type_id': 'int', 'play_type_description': 'string',
           'play_review_type_id': 'int', 'play_review_type_name': 'string',
           'play_reversed_on_review': 'bool'}
ADV_ROS = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int', 'cfl_central_id': 'int',
           'first_name': 'string', 'middle_name': 'string', 'last_name': 'string',
           'birth_date': 'datetime64[ns]', 'uniform': 'int', 'position': 'string',
           'is_national': 'bool', 'is_starter': 'bool', 'is_inactive': 'bool'}

ADV_BOX_TEAM = {}
ADV_BOX_TEAM['offence'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int',
                           'offence_possession_time': 'string', 'down_1_attempts': 'Int64',
                           'down_2_attempts': 'Int64', 'down_3_attempts': 'Int64',
                           'down_1_yards': 'Int64', 'down_2_yards': 'Int64',
                           'down_3_yards': 'Int64'}
ADV_BOX_TEAM['converts'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int',
                            'one_point_converts_attempts': 'Int64',
                            'one_point_converts_made': 'Int64',
                            'two_point_converts_attempts': 'Int64',
                            'two_point_converts_made': 'Int64'}
ADV_BOX_TEAM['passing'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int',
                           'pass_attempts': 'Int64',
                           'pass_completions': 'Int64', 'pass_net_yards': 'Int64', 'pass_long': 'Int64',
                           'pass_touchdowns': 'Int64', 'pass_completion_percentage': 'float',
                           'pass_efficiency': 'float',
                           'pass_interceptions': 'Int64', 'pass_fumbles': 'Int64'}
ADV_BOX_TEAM['penalties'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int', 'total': 'int',
                             'yards': 'int',
                             'offence_total': 'int', 'offence_yards': 'int', 'defence_total': 'int',
                             'defence_yards': 'int',
                             'special_teams_coverage_total': 'int', 'special_teams_coverage_yards': 'int',
                             'special_teams_return_total': 'int', 'special_teams_return_yards': 'int'}
ADV_BOX_TEAM['defence'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int',
                           'tackles_total': 'Int64',
                           'tackles_defensive': 'Int64', 'tackles_special_teams': 'Int64',
                           'sacks_qb_made': 'Int64',
                           'interceptions': 'Int64', 'fumbles_forced': 'Int64', 'fumbles_recovered': 'Int64',
                           'passes_knocked_down': 'Int64', 'defensive_touchdowns': 'Int64',
                           'defensive_safeties': 'Int64'}
ADV_BOX_TEAM['kicking'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int', 'kicks': 'Int64',
                           'kick_yards': 'Int64', 'kicks_net_yards': 'Int64', 'kicks_long': 'Int64',
                           'kicks_singles': 'Int64', 'kicks_out_of_end_zone': 'Int64', 'kicks_onside': 'Int64'}
ADV_BOX_TEAM['field_goal_returns'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int',
                                      'field_goal_returns': 'Int64',
                                      'field_goal_returns_yards': 'Int64',
                                      'field_goal_returns_touchdowns': 'Int64',
                                      'field_goal_returns_long': 'Int64',
                                      'field_goal_returns_touchdowns_long': 'Int64'}
ADV_BOX_TEAM['field_goals'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int',
                               'field_goal_attempts': 'Int64',
                               'field_goal_made': 'Int64', 'field_goal_yards': 'Int64',
                               'field_goal_singles': 'Int64',
                               'field_goal_long': 'Int64', 'field_goal_points': 'Int64'}
ADV_BOX_TEAM['kick_returns'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int',
                                'kick_returns': 'Int64',
                                'kick_returns_yards': 'Int64', 'kick_returns_touchdowns': 'Int64',
                                'kick_returns_long': 'Int64',
                                'kick_returns_touchdowns_long': 'Int64'}
ADV_BOX_TEAM['punt_returns'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int',
                                'punt_returns': 'Int64',
                                'punt_returns_yards': 'Int64', 'punt_returns_touchdowns': 'Int64',
                                'punt_returns_long': 'Int64',
                                'punt_returns_touchdowns_long': 'Int64'}
ADV_BOX_TEAM['punts'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int', 'punts': 'Int64',
                         'punt_yards': 'Int64', 'punt_net_yards': 'Int64', 'punt_long': 'Int64',
                         'punt_singles': 'Int64',
                         'punts_blocked': 'Int64', 'punts_in_10': 'Int64', 'punts_in_20': 'Int64',
                         'punts_returned': 'Int64'}
ADV_BOX_TEAM['receiving'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int',
                             'receive_attempts': 'Int64',
                             'receive_caught': 'Int64', 'receive_yards': 'Int64', 'receive_long': 'Int64',
                             'receive_touchdowns': 'Int64', 'receive_long_touchdowns': 'Int64',
                             'receive_yards_after_catch': 'Int64', 'receive_fumbles': 'Int64'}
ADV_BOX_TEAM['rushing'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int',
                           'rush_attempts': 'Int64',
                           'rush_net_yards': 'Int64', 'rush_long': 'Int64', 'rush_touchdowns': 'Int64',
                           'rush_long_touchdowns': 'Int64'}
ADV_BOX_TEAM['turnovers'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int', 'fumbles': 'Int64',
                             'interceptions': 'Int64', 'downs': 'Int64'}

ADV_BOX_PLAYER = {}
ADV_BOX_PLAYER['passing'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int',
                             'pass_attempts': 'int',
                             'pass_completions': 'int', 'pass_net_yards': 'int', 'pass_long': 'int',
                             'pass_touchdowns': 'int', 'pass_completion_percentage': 'float',
                             'pass_efficiency': 'float',
                             'pass_interceptions': 'int', 'pass_fumbles': 'int', 'cfl_central_id': 'int',
                             'first_name': 'string', 'middle_name': 'string', 'last_name': 'string',
                             'birth_date': 'string'}
ADV_BOX_PLAYER['rushing'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int',
                             'rush_attempts': 'int',
                             'rush_net_yards': 'int', 'rush_long': 'int', 'rush_touchdowns': 'int',
                             'rush_long_touchdowns': 'int', 'cfl_central_id': 'int', 'first_name': 'string',
                             'middle_name': 'string', 'last_name': 'string', 'birth_date': 'string'}
ADV_BOX_PLAYER['receiving'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int',
                               'receive_attempts': 'int',
                               'receive_caught': 'int', 'receive_yards': 'int', 'receive_long': 'int',
                               'receive_touchdowns': 'int', 'receive_long_touchdowns': 'int',
                               'receive_yards_after_catch': 'int', 'receive_fumbles': 'int',
                               'cfl_central_id': 'int',
                               'first_name': 'string', 'middle_name': 'string', 'last_name': 'string',
                               'birth_date': 'string'}
ADV_BOX_PLAYER['punts'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int', 'punts': 'int',
                           'punt_yards': 'int', 'punt_net_yards': 'int', 'punt_long': 'int',
                           'punt_singles': 'int',
                           'punts_blocked': 'int', 'punts_in_10': 'int', 'punts_in_20': 'int',
                           'punts_returned': 'int',
                           'cfl_central_id': 'int', 'first_name': 'string', 'middle_name': 'string',
                           'last_name': 'string',
                           'birth_date': 'string'}
ADV_BOX_PLAYER['punt_returns'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int',
                                  'punt_returns': 'int',
                                  'punt_returns_yards': 'int', 'punt_returns_touchdowns': 'int',
                                  'punt_returns_long': 'int',
                                  'punt_returns_touchdowns_long': 'int', 'cfl_central_id': 'int',
                                  'first_name': 'string',
                                  'middle_name': 'string', 'last_name': 'string', 'birth_date': 'string'}
ADV_BOX_PLAYER['kick_returns'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int',
                                  'kick_returns': 'int',
                                  'kick_returns_yards': 'int', 'kick_returns_touchdowns': 'int',
                                  'kick_returns_long': 'int',
                                  'kick_returns_touchdowns_long': 'int', 'cfl_central_id': 'int',
                                  'first_name': 'string',
                                  'middle_name': 'string', 'last_name': 'string', 'birth_date': 'string'}
ADV_BOX_PLAYER['field_goals'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int',
                                 'field_goal_attempts': 'int',
                                 'field_goal_made': 'int', 'field_goal_yards': 'int',
                                 'field_goal_singles': 'int',
                                 'field_goal_long': 'int', 'field_goal_missed_list': 'string',
                                 'field_goal_points': 'int',
                                 'cfl_central_id': 'int', 'first_name': 'string', 'middle_name': 'string',
                                 'last_name': 'string',
                                 'birth_date': 'string'}
ADV_BOX_PLAYER['field_goal_returns'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int',
                                        'field_goal_returns': 'Int64',
                                        'field_goal_returns_yards': 'Int64',
                                        'field_goal_returns_touchdowns': 'Int64',
                                        'field_goal_returns_long': 'Int64',
                                        'field_goal_returns_touchdowns_long': 'Int64',
                                        'cfl_central_id': 'Int64', 'first_name': 'string',
                                        'middle_name': 'string',
                                        'last_name': 'string',
                                        'birth_date': 'string'}
ADV_BOX_PLAYER['kicking'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int', 'kicks': 'int',
                             'kick_yards': 'int', 'kicks_net_yards': 'int', 'kicks_long': 'int',
                             'kicks_singles': 'int',
                             'kicks_out_of_end_zone': 'int', 'kicks_onside': 'int', 'cfl_central_id': 'int',
                             'first_name': 'string', 'middle_name': 'string', 'last_name': 'string',
                             'birth_date': 'string'}
ADV_BOX_PLAYER['one_point_converts'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int',
                                        'one_point_converts_attempts': 'int', 'one_point_converts_made': 'int',
                                        'cfl_central_id': 'int',
                                        'first_name': 'string', 'middle_name': 'string', 'last_name': 'string',
                                        'birth_date': 'string'}
ADV_BOX_PLAYER['two_point_converts'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int',
                                        'two_point_converts_made': 'int',
                                        'cfl_central_id': 'int', 'first_name': 'string',
                                        'middle_name': 'string',
                                        'last_name': 'string',
                                        'birth_date': 'string'}
ADV_BOX_PLAYER['defence'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int',
                             'tackles_total': 'int',
                             'tackles_defensive': 'int', 'tackles_special_teams': 'int', 'sacks_qb_made': 'int',
                             'interceptions': 'int', 'fumbles_forced': 'int', 'fumbles_recovered': 'int',
                             'passes_knocked_down': 'int', 'cfl_central_id': 'int', 'first_name': 'string',
                             'middle_name': 'string', 'last_name': 'string', 'birth_date': 'string'}
ABV_BOX = {"team": ADV_BOX_TEAM, "players": ADV_BOX_PLAYER}
ADV_TABLES = []
for name in ADV_BOX_TEAM:
    ADV_TABLES.append("team_" + name)
for name in ADV_BOX_PLAYER:
    ADV_TABLES.append("players_" + name)
ADV_TABLES += ['pbp', 'penalties', 'reviews', 'roster']

DRIVES = {'year': 'int', 'game_id': 'int', 'play_id': 'int', 'play_sequence': 'int', 'entry': 'int',
          'drive_id': 'int', 'drive_sequence': 'int', 'home': 'bool', 'won': 'bool',
          'kickoff': 'int', 'conv1': 'int', 'conv2': 'int', 'regular': 'int', 'ot': 'bool', 'quarter': 'int',
          'time_remaining': 'Int64', 'down': 'Int64', 'yards_to_go': 'Int64', 'distance': 'Int64',
          'score_diff': 'int', 'score_diff_calc': 'float', 'total_score': 'int', 'last_play': 'bool',
          'points_scored': 'int', 'points_scored_on_drive': 'int'}
EPA = {'year': 'int', 'game_id': 'int', 'play_id': 'int', 'play_sequence': 'int', 'entry': 'int',
       'ep': 'float', 'epa': 'float', 'team_1_ep': 'float', 'team_2_ep': 'float',
       'team_1_epa': 'float', 'team_2_epa': 'float',
       'wp': 'float', 'wpa': 'float', 'team_1_wp': 'float', 'team_2_wp': 'float',
       'team_1_wpa': 'float', 'team_2_wpa': 'float'}
GEI = {'year': 'int', 'game_id': 'int', 'gei': 'float', 'gsi': 'float', 'cbf': 'float', 'gei_pct': 'float',
       'gsi_pct': 'float', 'cbf_pct': 'float', 'gei_rank': 'int', 'gsi_rank': 'int', 'cbf_rank': 'int'}
ELO = {'year': 'int', 'game_id': 'int', 'team_1_team_id': 'int', 'team_2_team_id': 'int',
       'team_1_elo_season_in': 'float', 'team_1_elo_season_out': 'float', 'team_2_elo_season_in': 'float',
       'team_2_elo_season_out': 'float', 'team_1_elo_franchise_in': 'float', 'team_1_elo_franchise_out': 'float',
       'team_2_elo_franchise_in': 'float', 'team_2_elo_franchise_out': 'float'}
