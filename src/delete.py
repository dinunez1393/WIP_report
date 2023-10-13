# DELETE SQL queries
from alerts import *
import logging
from utilities import show_message, datetime_from_py_to_sql
from datetime import datetime as dt, timedelta


SUCCESS_OP = "The INSERT operation completed successfully"
SQL_D_ERROR = "There was an error in the DELETE query"
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.ERROR)


def delete_oldData(db_conn):
    """
    Delete function for clearing old WIP data
    :param db_conn: The connection to the database
    :return: Nothing
    :rtype: None
    """
    query = """
        DELETE FROM [SBILearning].[dbo].[DNun_tbl_Production_WIP_history]
        WHERE [WIP_SnapshotDate] < DATEADD(DAY, -200, GETDATE());
    """

    try:
        delete_start = dt.now()
        with db_conn.cursor() as cursor:
            print("Deleting old data...")
            cursor.execute(query)
    except Exception as e:
        print(repr(e))
        LOGGER.error(SQL_D_ERROR, exc_info=True)
        show_message(AlertType.FAILED)
    else:
        db_conn.commit()
        print(f"WIP data prior to {dt.now() - timedelta(days=185)} has been deleted successfully\n"
              f"T: {dt.now() - delete_start}\n")


def delete_allData(db_conn):
    """
    Delete function truncates the WIP table
    :param db_conn: The connection to the database
    :return: None
    """
    query = "DELETE FROM [SBILearning].[dbo].[DNun_tbl_Production_WIP_history]";

    try:
        with db_conn.cursor() as cursor:
            print("Deleting all data...")
            cursor.execute(query)
    except Exception as e:
        print(repr(e))
        LOGGER.error(SQL_D_ERROR, exc_info=True)
        show_message(AlertType.FAILED)
    else:
        db_conn.commit()
        print(f"WIP table has been cleared\n")
