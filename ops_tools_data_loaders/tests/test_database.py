import os
import sys

import pytest
from integrations.utils.database import (mysql_db_connections,
                                         run_sql_and_send_data, run_sql_files,
                                         send_data_to_webhook)
from integrations.utils.misc import yaml_config


@pytest.fixture
def mysql_config() -> dict:
    config = yaml_config(yaml_file_path=os.path.join(
        sys.path[0], 'tests', 'test_data_loaders_config.yaml'),
        env_file_path=os.path.join(sys.path[0], '.env'),
        strict_mode=False
    ).get('MYSQL_DB')

    return config


def test_mysql_db_conns(mysql_config) -> dict[object, any]:
    connections = mysql_db_connections(
        config=mysql_config, close_connections=True)

    assert connections is not None
    assert len(connections) == 2
    assert 'customer_success_freshdesk' in connections
    assert 'customer_success_salesforce' in connections
