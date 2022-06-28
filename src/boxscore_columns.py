
TEAM_COLUMNS = {}
TEAM_COLUMNS['offence'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int',
                           'offence_possession_time': 'string', 'down_1_attempts': 'Int64',
                           'down_2_attempts': 'Int64', 'down_3_attempts': 'Int64',
                           'down_1_yards': 'Int64', 'down_2_yards': 'Int64',
                           'down_3_yards': 'Int64'}
TEAM_COLUMNS['converts'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int',
                            'one_point_converts_attempts': 'Int64',
                            'one_point_converts_made': 'Int64',
                            'two_point_converts_attempts': 'Int64',
                            'two_point_converts_made': 'Int64'}
TEAM_COLUMNS['passing'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int', 'pass_attempts': 'Int64',
                           'pass_completions': 'Int64', 'pass_net_yards': 'Int64', 'pass_long': 'Int64',
                           'pass_touchdowns': 'Int64', 'pass_completion_percentage': 'float',
                           'pass_efficiency': 'float',
                           'pass_interceptions': 'Int64', 'pass_fumbles': 'Int64'}
TEAM_COLUMNS['penalties'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int', 'total': 'int',
                             'yards': 'int',
                             'offence_total': 'int', 'offence_yards': 'int', 'defence_total': 'int',
                             'defence_yards': 'int',
                             'special_teams_coverage_total': 'int', 'special_teams_coverage_yards': 'int',
                             'special_teams_return_total': 'int', 'special_teams_return_yards': 'int'}
TEAM_COLUMNS['defence'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int', 'tackles_total': 'Int64',
                           'tackles_defensive': 'Int64', 'tackles_special_teams': 'Int64', 'sacks_qb_made': 'Int64',
                           'interceptions': 'Int64', 'fumbles_forced': 'Int64', 'fumbles_recovered': 'Int64',
                           'passes_knocked_down': 'Int64', 'defensive_touchdowns': 'Int64',
                           'defensive_safeties': 'Int64'}
TEAM_COLUMNS['kicking'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int', 'kicks': 'Int64',
                           'kick_yards': 'Int64', 'kicks_net_yards': 'Int64', 'kicks_long': 'Int64',
                           'kicks_singles': 'Int64', 'kicks_out_of_end_zone': 'Int64', 'kicks_onside': 'Int64'}
TEAM_COLUMNS['field_goal_returns'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int',
                                      'field_goal_returns': 'Int64',
                                      'field_goal_returns_yards': 'Int64', 'field_goal_returns_touchdowns': 'Int64',
                                      'field_goal_returns_long': 'Int64', 'field_goal_returns_touchdowns_long': 'Int64'}
TEAM_COLUMNS['field_goals'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int',
                               'field_goal_attempts': 'Int64',
                               'field_goal_made': 'Int64', 'field_goal_yards': 'Int64', 'field_goal_singles': 'Int64',
                               'field_goal_long': 'Int64', 'field_goal_points': 'Int64'}
TEAM_COLUMNS['kick_returns'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int', 'kick_returns': 'Int64',
                                'kick_returns_yards': 'Int64', 'kick_returns_touchdowns': 'Int64',
                                'kick_returns_long': 'Int64',
                                'kick_returns_touchdowns_long': 'Int64'}
TEAM_COLUMNS['punt_returns'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int', 'punt_returns': 'Int64',
                                'punt_returns_yards': 'Int64', 'punt_returns_touchdowns': 'Int64',
                                'punt_returns_long': 'Int64',
                                'punt_returns_touchdowns_long': 'Int64'}
TEAM_COLUMNS['punts'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int', 'punts': 'Int64',
                         'punt_yards': 'Int64', 'punt_net_yards': 'Int64', 'punt_long': 'Int64',
                         'punt_singles': 'Int64',
                         'punts_blocked': 'Int64', 'punts_in_10': 'Int64', 'punts_in_20': 'Int64',
                         'punts_returned': 'Int64'}
TEAM_COLUMNS['receiving'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int', 'receive_attempts': 'Int64',
                             'receive_caught': 'Int64', 'receive_yards': 'Int64', 'receive_long': 'Int64',
                             'receive_touchdowns': 'Int64', 'receive_long_touchdowns': 'Int64',
                             'receive_yards_after_catch': 'Int64', 'receive_fumbles': 'Int64'}
TEAM_COLUMNS['rushing'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int', 'rush_attempts': 'Int64',
                           'rush_net_yards': 'Int64', 'rush_long': 'Int64', 'rush_touchdowns': 'Int64',
                           'rush_long_touchdowns': 'Int64'}
TEAM_COLUMNS['turnovers'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int', 'fumbles': 'Int64',
                             'interceptions': 'Int64', 'downs': 'Int64'}

PLAYER_COLUMNS = {}
PLAYER_COLUMNS['passing'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int', 'pass_attempts': 'int',
                             'pass_completions': 'int', 'pass_net_yards': 'int', 'pass_long': 'int',
                             'pass_touchdowns': 'int', 'pass_completion_percentage': 'float',
                             'pass_efficiency': 'float',
                             'pass_interceptions': 'int', 'pass_fumbles': 'int', 'cfl_central_id': 'int',
                             'first_name': 'string', 'middle_name': 'string', 'last_name': 'string',
                             'birth_date': 'string'}
PLAYER_COLUMNS['rushing'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int', 'rush_attempts': 'int',
                             'rush_net_yards': 'int', 'rush_long': 'int', 'rush_touchdowns': 'int',
                             'rush_long_touchdowns': 'int', 'cfl_central_id': 'int', 'first_name': 'string',
                             'middle_name': 'string', 'last_name': 'string', 'birth_date': 'string'}
PLAYER_COLUMNS['receiving'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int', 'receive_attempts': 'int',
                               'receive_caught': 'int', 'receive_yards': 'int', 'receive_long': 'int',
                               'receive_touchdowns': 'int', 'receive_long_touchdowns': 'int',
                               'receive_yards_after_catch': 'int', 'receive_fumbles': 'int', 'cfl_central_id': 'int',
                               'first_name': 'string', 'middle_name': 'string', 'last_name': 'string',
                               'birth_date': 'string'}
PLAYER_COLUMNS['punts'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int', 'punts': 'int',
                           'punt_yards': 'int', 'punt_net_yards': 'int', 'punt_long': 'int', 'punt_singles': 'int',
                           'punts_blocked': 'int', 'punts_in_10': 'int', 'punts_in_20': 'int', 'punts_returned': 'int',
                           'cfl_central_id': 'int', 'first_name': 'string', 'middle_name': 'string',
                           'last_name': 'string',
                           'birth_date': 'string'}
PLAYER_COLUMNS['punt_returns'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int', 'punt_returns': 'int',
                                  'punt_returns_yards': 'int', 'punt_returns_touchdowns': 'int',
                                  'punt_returns_long': 'int',
                                  'punt_returns_touchdowns_long': 'int', 'cfl_central_id': 'int',
                                  'first_name': 'string',
                                  'middle_name': 'string', 'last_name': 'string', 'birth_date': 'string'}
PLAYER_COLUMNS['kick_returns'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int', 'kick_returns': 'int',
                                  'kick_returns_yards': 'int', 'kick_returns_touchdowns': 'int',
                                  'kick_returns_long': 'int',
                                  'kick_returns_touchdowns_long': 'int', 'cfl_central_id': 'int',
                                  'first_name': 'string',
                                  'middle_name': 'string', 'last_name': 'string', 'birth_date': 'string'}
PLAYER_COLUMNS['field_goals'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int',
                                 'field_goal_attempts': 'int',
                                 'field_goal_made': 'int', 'field_goal_yards': 'int', 'field_goal_singles': 'int',
                                 'field_goal_long': 'int', 'field_goal_missed_list': 'string',
                                 'field_goal_points': 'int',
                                 'cfl_central_id': 'int', 'first_name': 'string', 'middle_name': 'string',
                                 'last_name': 'string',
                                 'birth_date': 'string'}
PLAYER_COLUMNS['field_goal_returns'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int',
                                        'field_goal_returns': 'int',
                                        'field_goal_returns_yards': 'int', 'field_goal_returns_touchdowns': 'int',
                                        'field_goal_returns_long': 'int', 'field_goal_returns_touchdowns_long': 'int',
                                        'cfl_central_id': 'int', 'first_name': 'string', 'middle_name': 'string',
                                        'last_name': 'string',
                                        'birth_date': 'string'}
PLAYER_COLUMNS['kicking'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int', 'kicks': 'int',
                             'kick_yards': 'int', 'kicks_net_yards': 'int', 'kicks_long': 'int',
                             'kicks_singles': 'int',
                             'kicks_out_of_end_zone': 'int', 'kicks_onside': 'int', 'cfl_central_id': 'int',
                             'first_name': 'string', 'middle_name': 'string', 'last_name': 'string',
                             'birth_date': 'string'}
PLAYER_COLUMNS['one_point_converts'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int',
                                        'one_point_converts_attempts': 'int', 'one_point_converts_made': 'int',
                                        'cfl_central_id': 'int',
                                        'first_name': 'string', 'middle_name': 'string', 'last_name': 'string',
                                        'birth_date': 'string'}
PLAYER_COLUMNS['two_point_converts'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int',
                                        'two_point_converts_made': 'int',
                                        'cfl_central_id': 'int', 'first_name': 'string', 'middle_name': 'string',
                                        'last_name': 'string',
                                        'birth_date': 'string'}
PLAYER_COLUMNS['defence'] = {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int', 'tackles_total': 'int',
                             'tackles_defensive': 'int', 'tackles_special_teams': 'int', 'sacks_qb_made': 'int',
                             'interceptions': 'int', 'fumbles_forced': 'int', 'fumbles_recovered': 'int',
                             'passes_knocked_down': 'int', 'cfl_central_id': 'int', 'first_name': 'string',
                             'middle_name': 'string', 'last_name': 'string', 'birth_date': 'string'}
