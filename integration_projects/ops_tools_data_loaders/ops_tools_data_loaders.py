import asyncio
import os
import sys

from integrations.data import freshdesk, salesforce
from integrations.utils.database import (mysql_db_connections,
                                         run_sql_and_send_data)
from integrations.utils.misc import create_project_directories, yaml_config
from integrations.utils.project_logger import logger


async def main() -> None:
    logger.info('<<BEGIN>>')

    # ------ Config ----------
    config = yaml_config(
        yaml_file_path=os.path.join(sys.path[0], "data_loaders_config.yaml"))

    project_dirs = create_project_directories(
        project_dirs=config.get('PROJECT_FOLDERS'))

    mysql_db_conns = mysql_db_connections(config=config.get('MYSQL_DB'))

    # ------ Extract & Load ----------
    integration_list = [
        freshdesk.run_freshdesk_integration(
            config=config.get('FRESHDESK_API'),
            db_conn=mysql_db_conns.get('customer_success_freshdesk'),
            archive_folder_path=project_dirs.get('DATA_FD_ARCHIVE_FOLDER_PATH')
        ),
        salesforce.run_salesforce_integration(
            config=config.get('SALESFORCE_API'),
            db_conn=mysql_db_conns.get('customer_success_salesforce'),
            archive_folder_path=project_dirs.get('DATA_SF_ARCHIVE_FOLDER_PATH')
        )
    ]

    await asyncio.gather(*integration_list)

    logger.info('Data extraction & loading complete')

    # ------ Transform & Send Data ----------
    run_sql_and_send_data(
        connection=mysql_db_conns, 
        sql_queries=config.get('SQL_QUERIES'), 
        webhooks=config.get('WEBHOOKS'), 
        env=config.get('ENV'))

    logger.info('<<END>>')


if __name__ == '__main__':
    asyncio.run(main())
