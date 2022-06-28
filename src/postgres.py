import os
import dotenv
import psycopg2
from psycopg2.extras import DictCursor

dotenv.load_dotenv()
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")

conn = None
try:
    with psycopg2.connect(
            host=POSTGRES_HOST,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            port=POSTGRES_PORT
    ) as conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            # DDL TRANSACTION
            '''
            cur.execute('DROP TABLE IF EXISTS employee')

            create_script = """ CREATE TABLE employee (
                                            id int PRIMARY KEY,
                                            name varchar(50) NOT NULL,
                                            sex varchar(1),
                                            salary int,
                                            dept_id varchar(10)) """
            cur.execute(create_script)
            insert_script = """ INSERT INTO employee (id, name, sex, salary, dept_id) VALUES (%s, %s, %s, %s, %s)"""
            insert_values = [(1, 'Usman', 'M', 100000, 'D2'), (2, 'Esther', 'F', 50000, 'D1'),
                             (3, 'Kingsley', 'M', 150000, 'D2'), (4, 'Toyyib', 'M', 103000, 'D1')]
            for value in insert_values:
                cur.execute(insert_script, value)
            update_script = """ UPDATE employee SET salary = 1.50 * salary"""
            cur.execute(update_script)
            '''
            select_script = """ SELECT * FROM employee """
            cur.execute(select_script)
            for record in cur.fetchall():
                print(record['name'], record['sex'], record['salary'])
except Exception as error:
    print(error)
finally:
    if conn is not None:
        conn.close()
