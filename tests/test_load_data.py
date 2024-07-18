import pytest
from unittest.mock import MagicMock
import psycopg2
from scripts.load_data import load_data

def test_load_data(mocker):
    # Mock connection and cursor
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    
    # Mock psycopg2.connect to return the mock connection
    mocker.patch('psycopg2.connect', return_value=mock_conn)
    mock_conn.cursor.return_value = mock_cursor

    # Call the load_data function
    load_data()

    # Verify if the data loading query was executed
    mock_cursor.execute.assert_called()
    assert mock_conn.commit.called
