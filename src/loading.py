# INSERT SQL queries or Dataframe to CSV
from tqdm import tqdm
from alerts import *
from datetime import datetime as dt
import logging
import multiprocessing
import pandas as pd
from utilities import show_message, items_to_SQL_values
from db_conn import make_connection


SUCCESS_OP = "The INSERT operation completed successfully"
SQL_I_ERROR = "There was an error in the INSERT query"
CHUNK_SIZE = 1_000
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.ERROR)

SERVER_NAME_sbi = 'WQMSDEV01'
DATABASE_NAME_sbi = 'SBILearning'


def load_wip_data(wip_df, to_csv=False, isServer=True):
    """
    Function to load new cleaned data to SQL table either indirectly, via CSV, or directly, via INSERT query
    :param wip_df: A dataframe containing the cleaned WIP records
    :type wip_df: pandas.Dataframe
    :param to_csv: A flag to indicate whether to load the data to a CSV file or not
    :param isServer: A flag to indicate whether the data in cleaned_wip_df is server data or not
    :return: None
    """
    # Process number
    if multiprocessing.current_process().name == "SR_Pro_1":
        pro_num = 1
    elif multiprocessing.current_process().name == "SR_Pro_2":
        pro_num = 2
    elif multiprocessing.current_process().name == "SR_Pro_3":
        pro_num = 3
    else:  # RE Process
        pro_num = ""

    if to_csv:  # Save a CSV file of the cleaned data
        wip_df[['DwellTime_calendar', 'DwellTime_working']] = wip_df[
            ['DwellTime_calendar', 'DwellTime_working']].map(lambda x: format(x, '.7f'))
        print(f"Creating ({pro_num}) {'SR' if isServer else 'RE'} WIP .csv file in the background...")
        wip_df.to_csv(f"../CleanedRecords_csv/wip_{'sr' if isServer else 're'}_records_{pro_num}.csv", index=False)
        print(f"CSV file for ({pro_num}) {'SR' if isServer else 'RE'} WIP created successfully\n")
    else:
        print(f"({pro_num}) INSERT Process:\n")

        wip_df[['DwellTime_calendar', 'DwellTime_working']] = wip_df[
            ['DwellTime_calendar', 'DwellTime_working']].map(lambda x: format(x, '.7f'))
        wip_df[['PackedIsLast_flag', 'PackedPreviously_flag']] = \
            wip_df[['PackedIsLast_flag', 'PackedPreviously_flag']].astype(int)
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
                                        desc=f"Parsing through ({pro_num}){'SR' if isServer else 'RE'} "
                                             f"WIP values (big chunk)"):
                    for item in list_object:
                        wip_values_listItem.extend(item)
                    wip_values_chunked.append(wip_values_listItem.copy())
                    wip_values_listItem.clear()
                for item in tqdm(wip_remaining, total=len(wip_remaining),
                                 desc=f"Parsing through ({pro_num}){'SR' if isServer else 'RE'} "
                                      f"WIP values (small chunk)"):
                    wip_values_remaining.extend(item)

                big_load = True
            else:  # Chunk size smaller than 1,000
                for item in tqdm(cleaned_wip_list, total=len(cleaned_wip_list),
                                 desc=f"Parsing through ({pro_num}){'SR' if isServer else 'RE'} "
                                      f"WIP values (small chunk)"):
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
                      ,[PackedIsLast_flag]
                      ,[PackedPreviously_flag]
                      ,[ExtractionDate]
                    )
                    VALUES
                    {};
            """
            # INSERT new records into DB
            try:
                insert_start = dt.now()
                # Establish DB Connections
                db_conn = make_connection(SERVER_NAME_sbi, DATABASE_NAME_sbi)
                with db_conn.cursor() as cursor:
                    if big_load:
                        upload_size = len(wip_values_chunked)

                        if pro_num == 2:  # SQL upload for SR data - Process 2
                            for wip_item in tqdm(wip_values_chunked, total=upload_size,
                                                 desc=f"INSERTING new ({pro_num}) {'SR' if isServer else 'RE'} "
                                                      f"WIP records in chunks"):
                                cursor.execute(insert_query.format(items_to_SQL_values(wip_item, isForUpdate=False)))
                        else:  # SQL upload for RE data and SR data - Processes 1 & 3
                            insert_start = dt.now()
                            # Flags for process progress
                            nickel = dime = dime_2 = quarter = dime_3 = dime_4 = half = dime_6 = quarter_3 = \
                                dime_8 = ninety = ninety_5 = True
                            progress_prompt = f"\n({pro_num}) {'SR' if isServer else 'RE'} WIP INSERT operation at "

                            print(f"\n({pro_num}) {'SR' if isServer else 'RE'} WIP INSERT operation is "
                                  f"running on the background. Progress will show intermittently\n")
                            for index, wip_item in enumerate(wip_values_chunked):
                                cursor.execute(insert_query.format(items_to_SQL_values(wip_item, isForUpdate=False)))

                                # Progress feedback
                                current_progress = (index + 1) / upload_size
                                if ninety_5 and current_progress >= 0.95:
                                    print(f"{progress_prompt}95% ({upload_size} items) T: {dt.now() - insert_start}")
                                    ninety_5 = False
                                elif ninety and current_progress >= 0.9:
                                    print(f"{progress_prompt}90% ({upload_size} items) T: {dt.now() - insert_start}")
                                    ninety = False
                                elif dime_8 and current_progress >= 0.8:
                                    print(f"{progress_prompt}80% ({upload_size} items) T: {dt.now() - insert_start}")
                                    dime_8 = False
                                elif quarter_3 and current_progress >= 0.75:
                                    print(f"{progress_prompt}75% ({upload_size} items) T: {dt.now() - insert_start}")
                                    quarter_3 = False
                                elif dime_6 and current_progress >= 0.6:
                                    print(f"{progress_prompt}60% ({upload_size} items) T: {dt.now() - insert_start}")
                                    dime_6 = False
                                elif half and current_progress >= 0.5:
                                    print(f"{progress_prompt}50% ({upload_size} items) T: {dt.now() - insert_start}")
                                    half = False
                                elif dime_4 and current_progress >= 0.4:
                                    print(f"{progress_prompt}40% ({upload_size} items) T: {dt.now() - insert_start}")
                                    dime_4 = False
                                elif dime_3 and current_progress >= 0.3:
                                    print(f"{progress_prompt}30% ({upload_size} items) T: {dt.now() - insert_start}")
                                    dime_3 = False
                                elif quarter and current_progress >= 0.25:
                                    print(f"{progress_prompt}25% ({upload_size} items) T: {dt.now() - insert_start}")
                                    quarter = False
                                elif dime_2 and current_progress >= 0.2:
                                    print(f"{progress_prompt}20% ({upload_size} items) T: {dt.now() - insert_start}")
                                    dime_2 = False
                                elif dime and current_progress >= 0.1:
                                    print(f"{progress_prompt}10% ({upload_size} items) T: {dt.now() - insert_start}")
                                    dime = False
                                elif nickel and current_progress >= 0.05:
                                    print(f"{progress_prompt}5% ({upload_size} items) T: {dt.now() - insert_start}")
                                    nickel = False
                            print(f"{progress_prompt}100%. Duration: {dt.now() - insert_start}\n")

                        # Insert remaining values
                        if len(wip_remaining) > 0:
                            print(f"Inserting an additional small size ({len(wip_remaining)} rows) "
                                  f"of ({pro_num}) {'SR' if isServer else 'RE'} WIP records in the background...")
                            cursor.execute(insert_query.format(items_to_SQL_values(
                                wip_values_remaining, isForUpdate=False, chunk_size=len(wip_remaining))))
                    else:  # Insert small chunk (less than 1,000 rows)
                        print(f"Inserting a small size ({len(cleaned_wip_list)} rows) of "
                              f"({pro_num}) {'SR' if isServer else 'RE'} WIP records in the background...")
                        cursor.execute(insert_query.format(items_to_SQL_values(
                            wip_values, isForUpdate=False, chunk_size=len(cleaned_wip_list))))
            except Exception as e:
                print(repr(e))
                LOGGER.error(SQL_I_ERROR, exc_info=True)
                show_message(AlertType.FAILED)
            else:
                db_conn.commit()
                print(f"\n({pro_num}) {'SR' if isServer else 'RE'} INSERT Operation ran successfully.\n"
                      f"T: {dt.now() - insert_start}\n")
            finally:
                # Close the DB connection
                db_conn.close()
        # If no new records in server_dw_list
        else:
            print("\nNo new records to INSERT\n")
