# UPDATE SQL queries or Dataframe to CSV
from tqdm import tqdm
from alerts import *
import pandas as pd
from utilities import show_message, datetime_from_py_to_sql, items_to_SQL_values
import logging
from datetime import datetime as dt


SUCCESS_OP = "The UPDATE operation completed successfully"
SQL_U_ERROR = "There was an error in the UPDATE query"
SUCCESS_MSG = "Number of raw objects cleaned: {0}. Number of new records updated: {1}"
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.ERROR)


def update_wip_data(db_conn, wip_updated, to_csv=False):
    """
    Function to update the shipment status flag of the WIP records
    :param db_conn: the connection to the database
    :param wip_updated: a list containing two dataframes. The first one is for shipped records, the second one is for
    not shipped
    :type wip_updated: list
    :param to_csv: a flag indicating if the records should be saved to CSV file
    :return: None
    """
    if to_csv:  # Save a CSV file of the updated WIP data
        print("Creating updated WIP .csv files in the background...")
        wip_updated[0].to_csv("../CleanedRecords_csv/wip_updatedShipped.csv", index=False)

        wip_updated[1].to_csv("../CleanedRecords_csv/wip_updatedNotShipped.csv", index=False)
        print("CSV files for updated WIP created successfully\n")
    else:
        if wip_updated[0].shape[0] > 0:
            wip_shipped_set = set(wip_updated[0]['SerialNumber'])
        else:
            wip_shipped_set = []
        if wip_updated[1].shape[0] > 0:
            wip_notShipped_set = set(wip_updated[1]['SerialNumber'])
        else:
            wip_notShipped_set = []

        update_shipped_query = f"""
            WITH SerialNumber_CTE AS (
                SELECT Value
                FROM (
                    VALUES
                        {items_to_SQL_values(wip_shipped_set)}
                ) AS Items(Value)
            )
        
            UPDATE o
            SET o.[NotShippedTransaction_flag] = 0,
                o.[LatestUpdateDate] = '{datetime_from_py_to_sql(dt.now())}'
            FROM [SBILearning].[dbo].[DNun_tbl_Production_WIP_history] AS o
            JOIN SerialNumber_CTE AS u
            ON o.[SerialNumber] = u.[Value]
        """

        update_notShipped_query = f"""
            WITH SerialNumber_CTE AS (
                SELECT Value
                FROM (
                    VALUES
                        {items_to_SQL_values(wip_notShipped_set)}
                ) AS Items(Value)
            )
        
            UPDATE o
            SET o.[NotShippedTransaction_flag] = 1,
                o.[LatestUpdateDate] = '{datetime_from_py_to_sql(dt.now())}'
            FROM [SBILearning].[dbo].[DNun_tbl_Production_WIP_history] AS o
            JOIN SerialNumber_CTE AS u
            ON o.[SerialNumber] = u.[Value]
        """
        if len(wip_shipped_set) > 0 or len(wip_notShipped_set) > 0:
            try:
                with db_conn.cursor() as cursor:
                    if len(wip_shipped_set) > 0:
                        update_start = dt.now()
                        print("UPDATING WIP records for shipped units in the background...")
                        cursor.execute(update_shipped_query)
                        print(f"UPDATE for WIP shipped records is complete. T: {dt.now() - update_start}")
                    else:
                        print("No new records to UPDATE for shipped units\n")

                    if len(wip_notShipped_set) > 0:
                        update_start = dt.now()
                        print("UPDATING WIP shipment flag for not shipped units in the background...")
                        cursor.execute(update_notShipped_query)
                        print(f"UPDATE for WIP unshipped records is complete. T: {dt.now() - update_start}")
                    else:
                        print("No new records to UPDATE for not shipped units\n")
            except Exception as e:
                print(repr(e))
                LOGGER.error(SQL_U_ERROR, exc_info=True)
                show_message(AlertType.FAILED)
            else:
                db_conn.commit()
                print("\nUPDATE operation ran successfully\n")
        else:
            print("\nNo new records to UPDATE\n")
                