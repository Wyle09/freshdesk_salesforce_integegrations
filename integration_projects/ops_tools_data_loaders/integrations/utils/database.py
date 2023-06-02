import os
import sys
from datetime import datetime

import mysql.connector
import pandas
import requests
import sqlparse
from numpy import array_split, nan
from sqlalchemy import create_engine, text

from integrations.utils.project_logger import logger


def mysql_db_connections(config: dict, close_connections: bool = False) -> dict[object, any]:
    '''
    Creates MYSQL connection engine for schemas. It will also create the db schema, if it does not
    exist.

    :param: config: YAML config "MYSQL_DB"
    :param: close_connections: bool - default False. If True, it will close the connections after creation.
    :return: dict {'schema_name': 'Connection engine'}
    '''
    # Create database/schemas if it does not exists
    conn = mysql.connector.connect(
        user=config.get('DB_USERNAME'),
        password=config.get('DB_PWD'),
        host=config.get('HOST')
    )

    for schema in config.get('SCHEMAS'):
        cursor = conn.cursor()
        cursor.execute(f'CREATE DATABASE IF NOT EXISTS {schema};')

    conn.close()

    # Connection engine for each schema
    connections = {}

    for schema in config.get('SCHEMAS'):
        conn_url = f"mysql+pymysql://{config.get('DB_USERNAME')}:{config.get('DB_PWD')}@{config.get('HOST')}/{schema}"
        conn = create_engine(conn_url)
        connections[schema] = conn

        logger.info(f"Connection made for: {schema}")

        if close_connections:
            conn.dispose()
            logger.info(f"Connection closed for: {schema}")

    return connections


def run_sql_files(config: list, connection: dict[object, any]) -> None:
    '''
    Executes sql scripts specified in the config YAML.

    :param: config: YAML config "SQL_QUERIES" list
    :param: connection: dict {'schema_name': 'Connection engine'}
    :return: None
    '''
    if not config:
        logger.info('Missing SQL Queries - Skip')
        return

    sql_queries = (file for file in config)

    for sql_query in sql_queries:
        sql_file = sql_query.get('SCRIPT')
        schema = sql_query.get('SCHEMA')
        sql_file_path = os.path.join(sys.path[0], "sql", sql_file)
        conn = connection.get(schema)

        with open(sql_file_path, 'r') as file:
            sql_raw = file.read()

        # Connection engine cannot execute sql with multiple queries, need to parse and execute
        # each query individually.
        queries = sqlparse.split(sqlparse.format(sql_raw, strip_comments=True))

        try:
            with conn.connect() as c:
                for query in queries:
                    c.execute(text(query))

                c.commit()
                logger.info(f"SQL file ran sucessfully: {sql_file_path}")

        except Exception as e:
            logger.error(f"SQL File Error: {sql_file_path}: {e}")


def send_data_to_webhook(config: list, connection: dict[object, any], env: str = 'stg') -> None:
    '''
    Sends data to webhooks. 

    :param: config: YAML config "WEBHOOKS" list
    :param: connection: dict {'schema_name': 'Connection engine'}
    :param: env: YAML or .env config "ENV" string "prod" | default "stg". Note - If env == 'stg', functions will not execute. "ENV" must be set to "prod"
    will not execute. "ENV" must be set to "prod"
    :return: None
    '''
    if not config:
        logger.info('Missing Webhooks - Skip')
        return

    if env == 'stg':
        logger.info(
            f"Cannot Send Data - Env is set to 'stg'")
        return

    webhooks = (webhook for webhook in config)

    for webhook in webhooks:
        webhook_type = webhook.get('TYPE')
        webhook_url = webhook.get('URL')
        schema = webhook.get('SCHEMA')
        query = webhook.get('QUERY')
        num_of_payloads = webhook.get('NUM_OF_PAYLOADS')
        time = webhook.get('TIME')
        conn = connection.get(schema)

        # Adding this to reduce the number of operations being sent to the webhook.
        # This way we can avoid going over the limit for our integromat subscription.
        if time != 0 and int(datetime.now().strftime('%H%M')) < time:
            logger.info(
                f"Data not Sent to Webhook: {webhook_type}: {webhook_url}: Will run after {time} UTC")
            continue

        try:
            sql_df = pandas.read_sql_query(
                query, conn).replace({nan: None})

            # Split data into smaller bundles based on the "NUM_OF_PAYLOADS" webhook YAML config
            list_of_dfs = (df for df in array_split(sql_df, num_of_payloads))

            for i, df in enumerate(list_of_dfs, start=1):
                response = requests.post(
                    url=webhook_url,
                    data=df.to_json(orient='table', index=False),
                    headers={'Content-Type': 'application/json'}
                )

                if response.status_code == 200:
                    logger.info(
                        f"Data Sent to Webhook : {webhook_type}: {webhook_url}: {i} out of {num_of_payloads} payloads")
                else:
                    logger.error(
                        f"Failed to Send Data Error: {webhook_type}: {webhook_url}: {response.status_code}: {response.text}")

        except Exception as e:
            logger.error(f"Webhook Error: {webhook_type}: {webhook_url}: {e}")


def run_sql_and_send_data(connection: dict[object, any], sql_queries: list, webhooks: list, env: str = 'stg') -> None:
    '''
    Wraps the execution of run_sql_files() & send_data_to_webhook().
    It ensures the connections are properly closed after execution.

    :param: connection: dict {'schema_name': 'Connection engine'}
    :param: sql_queries: YAML config "SQL_QUERIES" list
    :param: webhooks: YAML config "WEBHOOKS" list
    :param: env: YAML or .env config "ENV" string "prod" | default "stg". Note: If env == 'stg', functions will not execute. "ENV" must be set to "prod"
    :return: None
    '''
    try:
        run_sql_files(config=sql_queries, connection=connection)
        logger.info('Data transformation complete')

        send_data_to_webhook(config=webhooks, connection=connection, env=env)
        logger.info('Sending data complete')

    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        for conn in connection.values():
            conn.dispose()
        logger.info('Connections closed')
