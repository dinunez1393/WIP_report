# INSERT SQL queries or Dataframe to CSV
from tqdm import tqdm
from alerts import *
import logging
import pandas as pd
from utilities import show_message, items_to_SQL_values

SUCCESS_OP = "The INSERT operation completed successfully"
SQL_I_ERROR = "There was an error in the INSERT query"
CHUNK_SIZE = 1_000
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
        wip_df[['DwellTime_calendar', 'DwellTime_working']] = wip_df[
            ['DwellTime_calendar', 'DwellTime_working']].applymap(lambda x: format(x, '.7f'))
        print(f"Creating {'SR' if isServer else 'RE'} WIP .csv file in the background...")
        wip_df.to_csv(f"../CleanedRecords_csv/wip_{'sr' if isServer else 're'}_records.csv", index=False)
        print(f"CSV file for {'SR' if isServer else 'RE'} WIP created successfully\n")
    else:
        print("INSERT Process:\n")

        wip_df[['DwellTime_calendar', 'DwellTime_working']] = wip_df[
            ['DwellTime_calendar', 'DwellTime_working']].applymap(lambda x: format(x, '.7f'))
        wip_df[['NotShippedTransaction_flag', 'PackedPreviously_flag']] =\
            wip_df[['NotShippedTransaction_flag', 'PackedPreviously_flag']].astype(int)
        wip_df = wip_df.fillna('NULL')
        # Convert dataframe to tuples
        cleaned_wip_list = [tuple(row) for _, row in wip_df.iterrows()]

        if len(cleaned_wip_list) > 0:
            wip_values_chunked = []
            wip_values_listItem = []
            wip_remaining = []
            wip_values_remaining = []
            wip_values = []

            # Divide the list of tuples into chunks
            if len(cleaned_wip_list) > CHUNK_SIZE:
                num_chunks = len(cleaned_wip_list) // CHUNK_SIZE
                wip_chunks = [cleaned_wip_list[i * CHUNK_SIZE: ((i + 1) * CHUNK_SIZE)] for i in range(num_chunks)]
                wip_remaining = cleaned_wip_list[num_chunks * CHUNK_SIZE:]

                # Unify the tuples in the lists into single lists of just values
                for list_object in tqdm(wip_chunks, total=len(wip_chunks),
                                        desc=f"Parsing through {'SR' if isServer else 'RE'} WIP values (big chunk)"):
                    for item in list_object:
                        wip_values_listItem.extend(item)
                    wip_values_chunked.append(wip_values_listItem.copy())
                    wip_values_listItem.clear()
                for item in tqdm(wip_remaining, total=len(wip_remaining),
                                 desc=f"Parsing through {'SR' if isServer else 'RE'} WIP values (small chunk)"):
                    wip_values_remaining.extend(item)

                big_load = True
            else:  # Chunk size smaller than 1,000
                for item in tqdm(cleaned_wip_list, total=len(cleaned_wip_list),
                                 desc=f"Parsing through {'SR' if isServer else 'RE'} WIP values (small chunk)"):
                    wip_values.extend(item)
                big_load = False

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
                      ,[PackedPreviously_flag]
                      ,[ExtractionDate]
                    )
                    VALUES
                    {};
            """
            # INSERT new records into DB
            try:
                with db_conn.cursor() as cursor:
                    if big_load:
                        for wip_item in tqdm(wip_values_chunked, total=len(wip_values_chunked),
                                             desc=f"INSERTING new {'SR' if isServer else 'RE'} WIP records in chunks"):
                            cursor.execute(insert_query.format(items_to_SQL_values(wip_item, isForUpdate=False)))
                        # Insert remaining values
                        print(f"Inserting an additional small size ({len(wip_remaining)}) "
                              f"of {'SR' if isServer else 'RE'} WIP records in the background...")
                        cursor.execute(insert_query.format(items_to_SQL_values(
                            wip_values_remaining, isForUpdate=False, chunk_size=len(wip_remaining))))
                    else:  # Insert small chunk (less than 1,000 rows)
                        print(f"Inserting a small size ({len(cleaned_wip_list)}) of {'SR' if isServer else 'RE'} "
                              f"WIP records in the background...")
                        cursor.execute(insert_query.format(items_to_SQL_values(
                            wip_values, isForUpdate=False, chunk_size=len(cleaned_wip_list))))
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
