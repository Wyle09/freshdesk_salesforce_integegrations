import json
import os
from datetime import datetime

import pandas

from integrations.utils.misc import utc_date
from integrations.utils.project_logger import logger


def create_json_file(data: dict, date_file_name: str) -> None:
    '''
    Creates JSON files if data retrieval was successful. Files will be created in the project 
    "data" directory.

    :param: data: dict response content
    :param: date_file_name: date that will be used as part of the file name
    :return: None
    '''
    working_dir = f"{os.getcwd()}/data"

    if data.get('STATUS') == 'success' and data.get('DATA'):
        logger.info('Creating file')

        # Replace apostrophes foud within strings.
        json_file = json.loads(
            str(json.dumps(data.get('DATA'))).replace("'", "^").replace('\\"', ''))

        with open(f"{working_dir}/{data.get('TYPE')}_{date_file_name}.json", 'w') as file:
            json.dump(json_file, file)
            logger.info(f"File created : {file.name}")
    else:
        logger.warning(f"Cannot create file: {data}")


def load_json_files(endpoint_type: str, connection: object) -> list[dict]:
    '''
    Load JSON files in the project "data" directory into MYSQL "raw" tables.
    
    :param: endpoint_type: YAML config "ENDPOINTS"."TYPE"
    :param: connection: MYSQL Connection Engine
    :return: list of dicts [{'STATUS': 'imported', 'ENDPOINT_TYPE':, 'FILE': 'file name'}]
    '''
    working_dir = f"{os.getcwd()}/data"
    data_files = [f"{working_dir}/{file}" for file in os.listdir(working_dir)
                  if file.startswith(endpoint_type) & file.endswith('.json')]

    # Store first file values
    json_df = pandas.read_json(data_files[0], dtype=str)
    json_df['file'] = data_files[0]
    json_df['inserted_time'] = utc_date().get('CURRENT_DATE_UTC')

    # Check to see if there are more than 1 file for the given endpoint.
    if len(data_files) > 1:
        logger.info(f"Multiple files found: {data_files}")

        # Start at the second file
        for file in data_files[1:]:
            df = pandas.read_json(file, dtype=str)
            df['file'] = file
            df['inserted_time'] = utc_date().get('CURRENT_DATE_UTC')
            json_df = pandas.concat([json_df, df], sort=False)

    # Replace single quotes with doubles to keep it as a valid json string.
    json_df.replace({'\'': '"'}, regex=True, inplace=True)

    # Update values to make it a valid MYSQL json oject
    json_df.replace({'None': 'null'}, regex=True, inplace=True)
    json_df.replace({'False': '0'}, regex=True, inplace=True)
    json_df.replace({'True': '1'}, regex=True, inplace=True)

    json_df.to_sql(name=f"{endpoint_type}_raw", con=connection,
                   index=False, if_exists='replace')

    imported_files = []
    for file in data_files:
        imported_files.append(
            {'STATUS': 'imported', 'ENDPOINT_TYPE': endpoint_type, 'FILE': file})

    logger.info(f"Files uploaded: {imported_files}")

    return imported_files


def move_files(imported_files: list[dict], archive_path: str) -> None:
    '''
    Archive imported json files.
    
    :param: imported_files: list of dicts [{'STATUS': 'imported', 'ENDPOINT_TYPE':, 
    'FILE': 'file name'}]
    :param: archive_path: string "archive folder path"
    :return: None
    '''
    for file in imported_files:
        file_destination = f"{archive_path}/{os.path.basename(file.get('FILE'))}"
        os.replace(
            file.get('FILE'), file_destination)

        logger.info(f"Archiving File: {file.get('FILE')} to {file_destination}")


def delete_old_files(archive_directories: list) -> None:
    '''
    Deletes files older than 7 days that are in the archive directories
    
    :param: archive_directories: list of archive folder paths
    '''
    archived_files = []

    for dir in archive_directories:
        for file in os.listdir(dir):
            if file.endswith('.json'):
                archived_files.append(f"{dir}/{file}")

    for file in archived_files:
        min_file_date = datetime.strptime(
            utc_date(-7).get('INTERVAL_DATE'), '%Y-%m-%dT00:00:00Z')
        filetime = datetime.utcfromtimestamp(os.path.getmtime(file))

        if filetime < min_file_date:
            logger.info(f"Deleting Archived File: {file}")
            os.remove(file)
