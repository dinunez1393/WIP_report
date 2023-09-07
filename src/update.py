# UPDATE SQL queries or Dataframe to CSV
from tqdm import tqdm
from alerts import *
import pandas as pd
from utilities import show_message, datetime_from_py_to_sql
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
            wip_shipped_list = [tuple(row) for _, row in wip_updated[0].iterrows()]
        else:
            wip_shipped_list = []
        if wip_updated[1].shape[0] > 0:
            wip_notShipped_list = list(wip_updated[1]['SerialNumber'].unique())
        else:
            wip_notShipped_list = []

        update_shipped_query = """
            UPDATE [SBILearning].[dbo].[DNun_tbl_Production_WIP_history]
            SET [NotShippedTransaction_flag] = ?,
                [LatestUpdateDate] = ?
            WHERE [SerialNumber] = ?
        """
        update_notShipped_query = f"""
                UPDATE [SBILearning].[dbo].[DNun_tbl_Production_WIP_history]
                SET [NotShippedTransaction_flag] = 1,
                    [LatestUpdateDate] = '{datetime_from_py_to_sql(dt.now())}'
                WHERE [SerialNumber] = ?
        """
        if len(wip_shipped_list) > 0 or len(wip_notShipped_list) > 0:
            try:
                with db_conn.cursor() as cursor:
                    if len(wip_shipped_list) > 0:
                        for wip_instance in tqdm(wip_shipped_list, total=len(wip_shipped_list),
                                                 desc="UPDATING WIP records for shipped units"):
                            cursor.execute(update_shipped_query, wip_instance)
                    else:
                        print("No new records to UPDATE for shipped units\n")
                    if len(wip_notShipped_list) > 0:
                        for wip_instance in tqdm(wip_notShipped_list, total=len(wip_notShipped_list),
                                                 desc="UPDATING WIP shipment flag for not shipped units"):
                            cursor.execute(update_notShipped_query, wip_instance)
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
                