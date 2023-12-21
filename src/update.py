# UPDATE SQL queries or Dataframe to CSV
from alerts import *
from utilities import *
from datetime import datetime as dt
import sys


LOGGER = logger_creator('UPDATE_Error')


def update_shipmentFlag(db_conn, wip_unpacked):
    """
    Function to update the shipment status flag of the WIP records
    :param db_conn: the connection to the database
    :param wip_unpacked: a set containing the SNs that were unpacked and restored into the process flow
    :type wip_unpacked: set
    """
    update_shipped_query = f"""
        WITH shipped_CTE AS (
            SELECT DISTINCT [SerialNumber]
            FROM [SBIDev].[dbo].[tbl_Production_OngoingWIP_Actual]
            WHERE [PackedIsLast_flag] = 1
        )
    
        UPDATE o
        SET o.[PackedIsLast_flag] = 1,
            o.[LatestUpdateDate] = '{datetime_from_py_to_sql(dt.now())}'
        FROM [SBIDev].[dbo].[tbl_Production_OngoingWIP_Actual] AS o
        JOIN shipped_CTE AS u
        ON o.[SerialNumber] = u.[SerialNumber]
    """

    update_unpacked_query = f"""
        WITH SerialNumber_CTE AS (
            SELECT SerialNumber
            FROM (
                VALUES
                    {items_to_SQL_values(collection=wip_unpacked)}
            ) AS Items(SerialNumber)
        )
    
        UPDATE o
        SET o.[PackedIsLast_flag] = 0,
            o.[LatestUpdateDate] = '{datetime_from_py_to_sql(dt.now())}'
        FROM [SBIDev].[dbo].[tbl_Production_OngoingWIP_Actual] AS o
        JOIN SerialNumber_CTE AS u
        ON o.[SerialNumber] = u.[SerialNumber]
    """
    try:
        with db_conn.cursor() as cursor:
            update_start = dt.now()
            print("UPDATING WIP shipment flag for shipped units in the background...")
            cursor.execute(update_shipped_query)
            print(f"UPDATE for WIP shipped records is complete. T: {dt.now() - update_start}")

            if len(wip_unpacked) > 0:
                update_start = dt.now()
                print("UPDATING WIP shipment flag for unpacked units in the background...")
                cursor.execute(update_unpacked_query)
                print(f"UPDATE for WIP unpacked records is complete. T: {dt.now() - update_start}")
            else:
                print("No new records to UPDATE for unpacked and restored units\n")
    except Exception as e:
        print(repr(e))
        LOGGER.error(Messages.SQL_U_ERROR.value, exc_info=True)
        show_message(AlertType.FAILED)
        sys.exit()
    else:
        db_conn.commit()
        print(f"\n{Messages.SUCCESS_UPDATE_OP.value}\n")


def update_str_nulls(db_conn):
    """
    Function to update the columns [OrderType], [FactoryStatus] based if they have the string value 'NULL',
    then set them to actual NULL
    :param db_conn: the connection to the database
    """
    update_query = """
                UPDATE [SBIDev].[dbo].[tbl_Production_OngoingWIP_Actual]
                SET [OrderType] =
                    CASE
                        WHEN [OrderType] = 'NULL'
                        THEN NULL
                        ELSE [OrderType]
                    END
                ,[FactoryStatus] =
                    CASE
                        WHEN [FactoryStatus] = 'NULL'
                        THEN NULL
                        ELSE [FactoryStatus]
                    END
                ,[Customer] =
                    CASE
                        WHEN [Customer] = 'NULL'
                        THEN NULL
                        ELSE [Customer]
                    END
                ,[LatestUpdateDate] =
                   CASE
                        WHEN [LatestUpdateDate] = '1970-01-01 00:00'
                        THEN NULL
                        ELSE [LatestUpdateDate]
                    END;
    """
    try:
        update_start = dt.now()
        with db_conn.cursor() as cursor:
            print("Updating Order Type and Factory status NULL values in the background...")
            cursor.execute(update_query)
    except Exception as e:
        print(repr(e))
        LOGGER.error(Messages.SQL_U_ERROR.value, exc_info=True)
        show_message(AlertType.FAILED)
        sys.exit()
    else:
        db_conn.commit()
        print(f"\nUPDATE operation ran successfully\nT: {dt.now() - update_start}\n")
