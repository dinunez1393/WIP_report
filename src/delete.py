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
    print("Deleting old data...")
    query = """
        DELETE FROM [SBILearning].[dbo].[DNun_tbl_Production_WIP_history]
        WHERE [WIP_SnapshotDate] < DATEADD(DAY, -200, GETDATE());
    """

    try:
        with db_conn.cursor() as cursor:
            cursor.execute(query)
    except Exception as e:
        print(repr(e))
        LOGGER.error(SQL_D_ERROR, exc_info=True)
        show_message(AlertType.FAILED)
    else:
        db_conn.commit()
        print(f"WIP data prior to {dt.now() - timedelta(days=185)} has been deleted successfully\n")
