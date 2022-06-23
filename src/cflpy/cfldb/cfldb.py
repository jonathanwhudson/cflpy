import concurrent
import sys
import traceback
from typing import Union

import psycopg2
from psycopg2._psycopg import cursor
from psycopg2.extras import DictCursor
from psycopg2.pool import ThreadedConnectionPool

from cfldb.config import config

from cfldb.cfltables import table_creation_queries
from cfldb.util.helper import chunk


class CFLDB:
    instance = None  # type: CFLDB

    def __new__(cls, *args, **kwargs):
        if cls.instance is None:
            cls.instance = super().__new__(CFLDB)
            return cls.instance
        return cls.instance

    def __init__(self):
        self.name = config.db_name  # type: str
        try:
            self.db_pool = ThreadedConnectionPool(config.min_pool_size, config.max_pool_size, user=config.user,
                                                  password=config.password, host=config.host, port=config.port,
                                                  database=config.database)  # type: ThreadedConnectionPool
        except psycopg2.Error as error:
            print(error, file=sys.stderr)
            exit("Failed to connect to postgres database!")
        if not self.exists_db():
            self.create_db()
            print(f"{self.name} database created.")
        else:
            print(f"{self.name} database already exists.")

    def __del__(self):
        self.close()

    def close(self) -> bool:
        if self.db_pool is not None and not self.db_pool.closed:
            self.db_pool.closeall()
            return True
        return False

    def action(self, query: str) -> None:
        try:
            conn = self.db_pool.getconn()
            with conn.cursor() as curs:
                curs.execute(query)
            conn.commit()
            self.db_pool.putconn(conn)
        except psycopg2.Error as error:
            traceback.print_exc(file=sys.stderr)
            print(error, file=sys.stderr)
            sys.exit(f"Failed to execute action: {query}")

    def query(self, query: str, curs: cursor) -> None:
        try:
            return curs.execute(query)
        except psycopg2.Error as error:
            traceback.print_exc(file=sys.stderr)
            print(error, file=sys.stderr)
            sys.exit(f"Failed to execute query: {query}")

    def fetchone(self, query: str, dict_cursor: bool = False) -> object:
        conn = self.db_pool.getconn()
        if dict_cursor:
            curs = conn.cursor(cursor_factory=DictCursor)
        else:
            curs = conn.cursor()
        with curs:
            self.query(query, curs)
            result = curs.fetchone()[0]
            self.db_pool.putconn(conn)
            return result

    def fetchall(self, query: str, dict_cursor: bool = False) -> object:
        conn = self.db_pool.getconn()
        if dict_cursor:
            curs = conn.cursor(cursor_factory=DictCursor)
        else:
            curs = conn.cursor()
        with curs:
            self.query(query, curs)
            results = curs.fetchall()
            self.db_pool.putconn(conn)
            return results

    def exists_db(self) -> bool:
        query = f'''
            SELECT EXISTS (
                SELECT 
                    datname 
                FROM 
                    pg_database 
                WHERE 
                    datname='{self.name}'
            )
            '''
        return bool(self.fetchone(query))

    def create_db(self) -> None:
        self.action(f'''CREATE database {self.name};''')

    def exists_table(self, table_name: str) -> bool:
        query = f'''
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE  table_schema = 'public'
                AND    table_name   = '{table_name}'
            );
        '''
        return bool(self.fetchone(query))

    def reset_tables(self, tables: list[str]) -> None:
        for table in tables:
            self.drop_create_table(table)

    def drop_create_table(self, table: str) -> None:
        self.drop_table(table)
        self.create_table(table)

    def drop_table(self, table: str) -> None:
        if self.exists_table(table):
            print(f"Dropping table '{table}'.")
            self.action(f'''DROP TABLE {table} CASCADE;''')

    def create_table(self, table: str) -> None:
        if not self.exists_table(table):
            print(f"Creating table '{table}'.")
            self.action(table_creation_queries[table])

    def exists(self, table: str, category: str, match: str) -> bool:
        query = f'''
            SELECT FROM {table} 
            WHERE  {category} = '{match}';
        '''
        return self.fetchall(query) != []

    def threader_queries(self, list_to_thread: list[str]) -> None:
        args_lists = chunk(list_to_thread, config.max_pool_size)
        with concurrent.futures.ThreadPoolExecutor(max_workers=config.max_pool_size) as executor:
            future_list = []
            for args in args_lists:
                future = executor.submit(self.store_query_helper, args)
                future_list.append(future)
            for future in future_list:
                try:
                    future.result()
                except Exception:
                    traceback.print_exc(file=sys.stderr)
                    sys.exit("Error in threader_queries!")

    def store_query_helper(self, query_list: list[str]):
        if query_list:
            conn = self.db_pool.getconn()
            with conn.cursor() as curs:
                for single_query in query_list:
                    self.query(single_query, curs)
            conn.commit()
            self.db_pool.putconn(conn)

    def drop_games_id_join_table(self, table: str, year: str) -> None:
        query = f'''
        DELETE FROM {table}
        USING games
        WHERE
            {table}.game_id = games.game_id AND
            games.year = {year};
        '''
        self.action(query)

    def drop_games_id_join_tables(self, tables: list[str], year: str) -> None:
        for table in tables:
            self.drop_games_id_join_table(table, year)

    def drop_row_by_game(self, table, start, end):
        self.action(
            f'''DELETE FROM {table} e USING games g WHERE e.game_id=g.game_id AND g.year >= {start} AND g.year <= {end};''')

    def drop_rows_by_game(self, tables, start, end):
        for table in tables:
            self.drop_row_by_game(table, start, end)


def clean_input(value: Union[str, None]) -> str:
    if value is None:
        return 'null'
    elif value == "":
        return 'null'
    elif type(value) is str:
        return "'" + value + "'"
    return value


def clean_string(string: str) -> str:
    return string.replace("'", "''")


def transform_table_names(table_names: list[str]) -> str:
    names = str(table_names)
    names = names.replace("[", "")
    names = names.replace("]", "")
    names = names.replace("'", "")
    return names


cfldb = CFLDB()
