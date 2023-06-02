import json
import os
from typing import Any

from simple_salesforce import Salesforce, SalesforceLogin

from integrations.utils.file_management import (create_json_file,
                                                delete_old_files,
                                                load_json_files, move_files)
from integrations.utils.misc import utc_date
from integrations.utils.project_logger import logger


def get_salesforce_data(username: str, pwd: str, security_token: str, endpoint_info: dict) -> dict[str, Any]:
    '''
    Retrieves data from Salesforce API for the given "SOQL" query.

    :param: username
    :param: pwd
    :param: security_token: Aka API Key
    :param: endpoint_info: values from yaml config SALESFORCE_API.ENDPOINTS
    :return: Dict {"STATUS": "error" | "pending" | "success", "TYPE": values from yaml config
    SALESFORCE_API.ENDPOINTS.TYPE, "DATA": API response or default message.}
    '''
    data_files = [f for f in os.listdir(f"{os.getcwd()}/data")
                  if f.startswith(endpoint_info.get('TYPE')) & f.endswith('.json')]

    # Check if there are any files pending for import
    if data_files:
        data = {
            'STATUS': 'pending',
            'TYPE': endpoint_info.get('TYPE'),
            'DATA': 'Existing file pending for import, Check data folder'
        }
        logger.info(data)
        return(data)

    try:
        session_id, instance = SalesforceLogin(
            username=username, password=pwd, security_token=security_token)

        sf = Salesforce(instance=instance, session_id=session_id)

        # Pagination logic
        response = sf.query(query=endpoint_info.get('SOQL'), include_deleted=False)
        response_content = response.get('records')
        next_url = response.get('nextRecordsUrl')

        while not response.get('done'):
            response = sf.query_more(next_url, identifier_is_url=True)
            response_content.extend(response.get('records'))
            next_url = response.get('nextRecordsUrl')

        data = {
            'STATUS': 'success',
            'TYPE': endpoint_info.get('TYPE'),
            'DATA': json.loads(json.dumps(response_content))
        }
        return data

    except Exception as e:
        data = {
            'STATUS': 'error',
            'TYPE': endpoint_info.get('TYPE'),
            'DATA': e
        }
        logger.error(data)
        return data


async def run_salesforce_integration(config: dict, db_conn: object, archive_folder_path: str) -> None:
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

    sf_ednpoints = (
        endpoint for endpoint in config.get('ENDPOINTS'))

    try:
        for endpoint in sf_ednpoints:
            sf_data = get_salesforce_data(
                username=config.get('USERNAME'),
                pwd=config.get('PWD'),
                security_token=config.get('SECURITY_TOKEN'),
                endpoint_info=endpoint
            )

            create_json_file(sf_data, utc_date().get('CURRENT_DATE_UTC'))

            imported_files = load_json_files(
                endpoint_type=endpoint.get('TYPE'),
                connection=db_conn
            )

            move_files(imported_files=imported_files,
                       archive_path=archive_folder_path)
    except Exception as e:
        logger.error(f"Integration error: {e}: {sf_data}")
        return

    delete_old_files(archive_directories=[archive_folder_path])

    logger.info('Salesforce extraction & loading complete')
