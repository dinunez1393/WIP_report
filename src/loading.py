# INSERT SQL queries or Dataframe to CSV
from tqdm import tqdm
from alerts import *
import logging
import pandas as pd
from utilities import show_message


SUCCESS_OP = "The INSERT operation completed successfully"
SQL_I_ERROR = "There was an error in the INSERT query"
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.ERROR)


def load_wip_data(db_conn, wip_df, to_csv=False, isServer=True):
    """
    Function to load new cleaned data to SQL table either indirectly, via CSV, or directly, via INSERT query
    :param db_conn: The connection to the database
    :param wip_df: A dataframe containing the cleaned WIP records
    :type wip_df: pandas.Dataframe
    :param to_csv: A flag to indicate whether to load the data to a CSV file or not
    :param isServer: A flag to indicate whether the data in cleaned_wip_df is server data or not
    :return: None
    """
    if to_csv:  # Save a CSV file of the cleaned data
        wip_df['DwellTime_calendar'] = wip_df['DwellTime_calendar'].apply(lambda row: format(row, '.9f'))
        wip_df['DwellTime_working'] = wip_df['DwellTime_working'].apply(lambda row: format(row, '.9f'))
        if isServer:
            print("Creating SR WIP .csv file in the background...")
            wip_df.to_csv("../CleanedRecords_csv/wip_sr_records.csv", index=False)
            print("CSV file for SR WIP created successfully\n")
        else:
            print("Creating RE WIP .csv file in the background...")
            wip_df.to_csv("../CleanedRecords_csv/wip_re_records.csv", index=False)
            print("CSV file for RE WIP created successfully\n")
    else:
        cleaned_wip_list = [tuple(row) for _, row in wip_df.iterrows()]
        insert_query = """
                INSERT INTO [SBILearning].[dbo].[DNun_tbl_Production_WIP_history] (
                    [Site]
                  ,[Building]
                  ,[SerialNumber]
                  ,[StockCode]
                  ,[SKU]
                  ,[CheckpointID]
                  ,[CheckpointName]
                  ,[ProcessArea]
                  ,[TransactionID]
                  ,[TransactionDate]
                  ,[WIP_SnapshotDate]
                  ,[DwellTime_calendar]
                  ,[DwellTime_working]
                  ,[OrderType]
                  ,[FactoryStatus]
                  ,[ProductType]
                  ,[NotShippedTransaction_flag]
                  ,[ExtractionDate]
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        if len(cleaned_wip_list) > 0:
            # INSERT new records into DB
            try:
                with db_conn.cursor() as cursor:
                    for wip_instance in tqdm(cleaned_wip_list, total=len(cleaned_wip_list),
                                             desc=f"INSERTING new {'SR' if isServer else 'RE'} WIP records"):
                        cursor.execute(insert_query, wip_instance)

            except Exception as e:
                print(repr(e))
                LOGGER.error(SQL_I_ERROR, exc_info=True)
                show_message(AlertType.FAILED)
            else:
                db_conn.commit()
                print("\nINSERT Operation ran successfully\n")
        # If no new records in server_dw_list
        else:
            print("\nNo new records to INSERT\n")
