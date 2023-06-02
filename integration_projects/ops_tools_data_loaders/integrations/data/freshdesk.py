import json
import os
import re
from typing import Any

import requests

from integrations.utils.file_management import (create_json_file,
                                                delete_old_files,
                                                load_json_files, move_files)
from integrations.utils.misc import utc_date
from integrations.utils.project_logger import logger


def get_freshtdesk_data(api_key: str, pwd: str, endpoint_info: dict, updated_since_utc: str = None) -> dict[str, Any]:
    '''
    Retrieves data from Freshdesk API based on the given endpoint.

    :param: api_key
    :param: pwd
    :param: endpoint_info: values from yaml config FRESHDESK_API.ENDPOINTS
    :return: Dict {"STATUS": "error" | "pending" | "success", "TYPE": values from yaml config
    FRESHDESK_API.ENDPOINTS.TYPE, "DATA": API response or default message.}
    '''
    data_files = [f for f in os.listdir(f"{os.getcwd()}/data")
                  if f.startswith(endpoint_info.get('TYPE')) and f.endswith('.json')]

    # Check if there are any files pending for import
    if data_files:
        data = {
            'STATUS': 'pending',
            'TYPE': endpoint_info.get('TYPE'),
            'URL': endpoint_info.get('URL'),
            'DATA': 'Existing file pending for import. Check data folder.'
        }
        logger.info(data)
        return data

    # Check if endpoint URL requires a time filter.
    if 'updated_since=' in endpoint_info.get('URL'):
        endpoint_info['URL'] = f"{endpoint_info.get('URL')}{updated_since_utc}"

    response = requests.get(url=endpoint_info.get('URL'), auth=(api_key, pwd))
    print(response)

    try:
        if response.status_code == 200:
            response_content = json.loads(response.content)

            if 'Link' in response.headers:
                # Pagination is needed
                logger.info('Requests processed successfully (Multiple pages)')
                next_url = re.findall(
                    "<(.+?)>", response.headers.get('Link'))[0]
                while next_url:
                    response = requests.get(url=next_url, auth=(api_key, pwd))
                    response_content.extend(json.loads(response.content))

                    if 'Link' in response.headers:
                        next_url = re.findall(
                            "<(.+?)>", response.headers.get('Link'))[0]
                    else:
                        logger.info(f"Last page URL = {next_url}")
                        next_url = None
            else:
                # Pagination is not needed
                logger.info('Requests processed successfully (1 page)')

            data = {
                'STATUS': 'success',
                'TYPE': endpoint_info.get('TYPE'),
                'URL': endpoint_info.get('URL'),
                'DATA': response_content
            }
        else:
            # Error response
            data = {
                'STATUS': 'error',
                'TYPE': endpoint_info.get('TYPE'),
                'URL': endpoint_info.get('URL'),
                'DATA': response
            }
            logger.error(data)

    except Exception as e:
        data = {
            'STATUS': 'error',
            'TYPE': endpoint_info.get('TYPE'),
            'URL': endpoint_info.get('URL'),
            'DATA': response
        }
        logger.error(f"{e}: {data}")

    return data


async def run_freshdesk_integration(config: dict, db_conn: object, archive_folder_path: str) -> None:
    '''
    Executes the following sequences for each endpoint type specified in the yaml config. Get data
    >> Create JSON file >> Import JSON file >> Archive File >> Delete Old Files.

    :param: config: yaml config "SALESFORCE_API" dict
    :param: db_conn: Connection object for customer_success_salesforce schema
    :param: archive_folder_path: file archive folder path.
    :return: None
    '''
    if not config.get('ENDPOINTS'):
        logger.info('Missing endpoints - Skip Integration')
        return

    fd_endpoints = (
        endpoint for endpoint in config.get('ENDPOINTS'))

    # UTC Timestamp for time range filter
    fd_updated_since_utc = utc_date(
        interval_num=int(config.get('INTERVAL_DAYS'))).get('INTERVAL_DATE')

    try:
        for endpoint in fd_endpoints:
            fd_data = get_freshtdesk_data(
                api_key=config.get('API_KEY'),
                pwd=config.get('PWD'),
                endpoint_info=endpoint,
                updated_since_utc=fd_updated_since_utc
            )

            if not fd_data.get('DATA'):
                logger.warning(f"{fd_data} No data to process - Skip")
                continue

            # Loading files to Staging tables
            create_json_file(fd_data, utc_date().get('CURRENT_DATE_UTC'))
            imported_files = load_json_files(
                endpoint_type=endpoint.get('TYPE'),
                connection=db_conn
            )

            move_files(imported_files=imported_files,
                       archive_path=archive_folder_path)
    except Exception as e:
        logger.error(f"Integration error: {e}: {fd_data}")
        return

    delete_old_files(archive_directories=[archive_folder_path])

    logger.info('Freshdesk extraction & loading complete')
