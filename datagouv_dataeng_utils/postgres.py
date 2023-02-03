import psycopg2
from typing import List, TypedDict
import os

class File(TypedDict):
    source_name: str
    source_path: str


def get_conn(
    PG_HOST: str,
    PG_PORT: str,
    PG_DB: str,
    PG_USER: str,
    PG_PASSWORD: str,
):
    conn = psycopg2.connect(
        host=PG_HOST,
        database=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        port=PG_PORT
    )
    return conn


def return_sql_results(cur):
    try:
        data = cur.fetchall()
    except: 
        data = None
    if data:
        columns = [desc[0] for desc in cur.description]
        return [{ k:v for k,v in zip(columns, d)} for d in data]
    else:
        return True


def execute_query(
    PG_HOST: str,
    PG_PORT: str,
    PG_DB: str,
    PG_USER: str,
    PG_PASSWORD: str,
    sql: str
):
    conn = get_conn(PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD)
    with conn.cursor() as cur:
        cur.execute(sql)
        data = return_sql_results(cur)
        conn.commit()
        conn.close()
    return data


def execute_sql_file(
    PG_HOST: str,
    PG_PORT: str,
    PG_DB: str,
    PG_USER: str,
    PG_PASSWORD: str,
    list_files: List[File],
):
    for file in list_files:
        is_file = os.path.isfile(
            os.path.join(file['source_path'], file['source_name'])
        )
        if is_file:
            conn = get_conn(PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD)
            with conn.cursor() as cur:
                cur.execute(
                    open(
                        os.path.join(file['source_path'], file['source_name']), "r"
                    ).read()
                )
                data = return_sql_results(cur)
                conn.commit()
                conn.close()
        else:
            raise Exception(f"file {file['source_path']}{file['source_name']} does not exists") 
    return data


def copy_file(
    PG_HOST: str,
    PG_PORT: str,
    PG_DB: str,
    PG_TABLE: str,
    PG_USER: str,
    PG_PASSWORD: str,
    list_files: List[File],
):
    for file in list_files:
        is_file = os.path.isfile(
            os.path.join(file['source_path'], file['source_name'])
        )
        if is_file:
            conn = get_conn(PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD)
            sql = "COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS ','"
            file = open(os.path.join(file['source_path'], file['source_name']), "r")
            with conn.cursor() as cur:
                cur.copy_expert(sql=sql % PG_TABLE, file=file)
                data = return_sql_results(cur)
                conn.commit()
                conn.close()
        else:
            raise Exception(f"file {file['source_path']}{file['source_name']} does not exists") 
    return data
