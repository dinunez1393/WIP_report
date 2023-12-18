# DELETE SQL queries
from alerts import *
from utilities import show_message, logger_creator
from datetime import datetime as dt, timedelta


LOGGER = logger_creator('DELETE_Error')


def delete_oldData(db_conn):
    """
    Delete function for clearing old WIP data
    :param db_conn: The connection to the database
    """
    query = """
        DELETE FROM [SBILearning].[dbo].[DNun_tbl_Production_OngoingWIP_Actual]
        WHERE [WIP_SnapshotDate] < DATEADD(DAY, -400, GETDATE());
    """

    try:
        delete_start = dt.now()
        with db_conn.cursor() as cursor:
            print("Deleting old data...")
            cursor.execute(query)
    except Exception as e:
        print(repr(e))
        LOGGER.error(Messages.SQL_D_ERROR.value, exc_info=True)
        show_message(AlertType.FAILED)
    else:
        db_conn.commit()
        print(f"WIP data prior to {dt.now() - timedelta(days=185)} has been deleted successfully\n"
              f"T: {dt.now() - delete_start}\n")


def delete_allData(db_conn):
    """
    Delete function truncates the WIP table
    :param db_conn: The connection to the database
    """
    query = "TRUNCATE TABLE [SBILearning].[dbo].[DNun_tbl_Production_OngoingWIP_Actual];"

    try:
        delete_start = dt.now()
        with db_conn.cursor() as cursor:
            print("Deleting all data...")
            cursor.execute(query)
    except Exception as e:
        print(repr(e))
        LOGGER.error(Messages.SQL_D_ERROR.value, exc_info=True)
        show_message(AlertType.FAILED)
    else:
        db_conn.commit()
        print(f"WIP table has been cleared\nT: {dt.now() - delete_start}\n")
