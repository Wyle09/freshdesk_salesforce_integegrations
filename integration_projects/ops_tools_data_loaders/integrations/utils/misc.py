import os
from datetime import datetime, timedelta, timezone
from typing import Any

from envyaml import EnvYAML
from integrations.utils.project_logger import logger


def utc_date(interval_num: int = 0) -> dict[str, str]:
    '''
    Get current & interval utc date which can be use for api endpoint filtering or file names. 

    :param: interval_num: dafault 0
    :return: dict {"INTERVAL_DATE", "CURRENT_DATE_UTC"}
    '''
    interval_date_utc = (datetime.now(tz=timezone.utc) + timedelta(days=interval_num)
                         ).strftime('%Y-%m-%dT00:00:00Z')

    current_date_utc = datetime.now(
        tz=timezone.utc).strftime('%Y%m%d%H%M%S')

    return {
        'INTERVAL_DATE': interval_date_utc,
        'CURRENT_DATE_UTC': current_date_utc
    }


def yaml_config(yaml_file_path: str, env_file_path: str = f"{os.getcwd()}/.env", strict_mode: bool = True) -> object:
    '''
    Parses YAML file and add env variables declared in .env file or local machine. If .env file
    is not found in the project working directory then it will look for the global env variables
    configured in your machine.

    :param: yaml_file_path: yaml config file path
    :param: env_file_path: default .env file path in project working directory
    :param: stric_mode: default False. If True, it will raise an error if env variables are not
    :return: YAML config object yaml & env variables values
    '''

    if os.path.exists(env_file_path):
        data = EnvYAML(yaml_file=yaml_file_path,
                       env_file=env_file_path, strict=strict_mode)
        logger.info('.env file found')
    else:
        data = EnvYAML(yaml_file=yaml_file_path, strict=strict_mode)
        logger.info('.env file not found - defaulting to machine env variables')

    if not data:
        logger.error('Missing yaml config file')

    return data


def create_project_directories(project_dirs: dict) -> dict:
    '''
    Creates folders specified in yaml config.

    :param: project_dirs: yaml config "PROJECT_FOLDERS" dict
    :return: dict {"FOLDER_NAME": "PATH"}. Note: key names will be upper case:
    {FOLDER NAME}_FOLDER_PATH for parent directories. For child directories: 
    {PARENT FOLDER NAME}_{CHILD FOLDER NAME}_FOLDER_PATH.
    '''
    working_dir = os.getcwd()
    paths = {}

    # Create parent directories if it does not exist
    for dir in project_dirs['PARENT_DIRECTORIES']:
        path = os.path.join(working_dir, dir)
        os.makedirs(path, exist_ok=True)
        paths[f"{dir.upper()}_FOLDER_PATH"] = path

    # Create child directories if it does not exist
    for parent_dir in project_dirs['CHILD_DIRECTORIES']:
        for key in parent_dir.keys():
            for child_dir in parent_dir[key]:
                path = os.path.join(f"{working_dir}/{key}", child_dir)
                os.makedirs(path, exist_ok=True)
                paths[f"{key.upper()}_{child_dir.upper()}_FOLDER_PATH"] = path

    if not paths:
        logger.info('Missing project direcotries')

    return paths
