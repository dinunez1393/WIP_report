# INSERT SQL queries or Dataframe to CSV
from tqdm import tqdm
from alerts import *
from datetime import datetime as dt
import multiprocessing
import pandas as pd
import time as ti
from utilities import *
from db_conn import make_connection
import sys


CHUNK_SIZE = 1_000
LOGGER = logger_creator('INSERT_Error')

SERVER_NAME_sbi = 'WQMSDEV01'
DATABASE_NAME_sbi = 'SBILearning'


def load_wip_data(wip_df, semaphore, to_csv=False, isServer=True):
    """
    Function to load new cleaned data to SQL table either indirectly, via CSV, or directly, via INSERT query.
    For SQL INSERT: The data is first inserted to temporary tables carrying in their names the process number, hence
    allowing pseudo-parallel insertion.
    Then, all the data from the temp table is bulk inserted into the main SQL WIP table.
    :param wip_df: A dataframe containing the cleaned WIP records
    :type wip_df: pandas.DataFrame
    :param semaphore: A semaphore for the process that will be used to upload the data to SQL
    :type semaphore: multiprocessing.Semaphore
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
    elif multiprocessing.current_process().name == "SR_Pro_4":
        pro_num = 4
    else:  # RE Process
        pro_num = 5

    if to_csv:  # Save a CSV file of the cleaned data
        wip_df[['DwellTime_calendar', 'DwellTime_working']] = wip_df[
            ['DwellTime_calendar', 'DwellTime_working']].applymap(lambda x: format(x, '.7f'))
        print(f"Creating ({pro_num}) {'SR' if isServer else 'RE'} WIP .csv file in the background...")
        wip_df.to_csv(f"CleanedRecords_csv/wip_{'sr' if isServer else 're'}_records_{pro_num}.csv", index=False)
        print(f"CSV file for ({pro_num}) {'SR' if isServer else 'RE'} WIP created successfully\n")
    else:
        print(f"({pro_num}) {'SR' if isServer else 'RE'} INSERT Process:\n")

        wip_df[['DwellTime_calendar', 'DwellTime_working']] = wip_df[
            ['DwellTime_calendar', 'DwellTime_working']].applymap(lambda x: format(x, '.7f'))
        wip_df[['PackedIsLast_flag', 'PackedPreviously_flag', 'VoidSN_Previously_flag', 'ReworkScanPreviously_flag']] =\
            wip_df[['PackedIsLast_flag', 'PackedPreviously_flag',
                    'VoidSN_Previously_flag', 'ReworkScanPreviously_flag']].astype(int)
        wip_df = wip_df.fillna('NULL')
        # Epoch time is the dummy NULL for a datetime column
        wip_df.loc[(wip_df['Updated_time'] == 'NULL'), 'Updated_time'] = '1970-01-01 00:00'

        # Convert dataframe to chunked lists of values
        unravel_dfs = unravel_df_to_chunks(wip_df, CHUNK_SIZE)
        num_cols = wip_df.shape[1]

        if unravel_dfs is not None:
            create_query_temp = f"""
                    CREATE TABLE [SBILearning].[dbo].temp_tbl_Production_WIP_history_{pro_num}(
                    [Site] [char](2) NOT NULL,
                    [Building] [varchar](7) NOT NULL,
                    [SerialNumber] [char](12) NOT NULL,
                    [StockCode] [varchar](16) NOT NULL,
                    [StockCodePrefix] [char](2) NOT NULL,
                    [SKU] [varchar](100) NOT NULL,
                    [CheckpointID] [int] NOT NULL,
                    [CheckpointName] [varchar](50) NOT NULL,
                    [ProcessArea] [varchar](20) NOT NULL,
                    [TransactionID] [bigint] NOT NULL,
                    [TransactionDate] [datetime] NOT NULL,
                    [WIP_SnapshotDate] [datetime] NOT NULL,
                    [SnapshotTime] [varchar](4) NOT NULL,
                    [DwellTime_calendar] [decimal](9, 4) NOT NULL,
                    [DwellTime_working] [decimal](9, 4) NOT NULL,
                    [OrderType] [varchar](20) NULL,
                    [FactoryStatus] [varchar](20) NULL,
                    [ProductType] [varchar](8) NOT NULL,
                    [Customer] [varchar](5) NULL,
                    [PackedIsLast_flag] [bit] NULL,
                    [PackedPreviously_flag] [bit] NOT NULL,
                    [VoidSN_Previously_flag] [bit] NOT NULL,
                    [ReworkScanPreviously_flag] [bit] NOT NULL,
                    [ExtractionDate] [datetime] NOT NULL,
                    [LatestUpdateDate] [datetime] NULL);
            """
            insert_query_temp = """
                    INSERT INTO [SBILearning].[dbo].temp_tbl_Production_WIP_history_{0}
                    VALUES
                    {1};
            """
            drop_query_temp = f"DROP TABLE [SBILearning].[dbo].temp_tbl_Production_WIP_history_{pro_num};"
            insert_query_main = f"""
                    INSERT INTO [SBIDev].[dbo].[tbl_Production_OngoingWIP_Actual]
                    SELECT * FROM [SBILearning].[dbo].temp_tbl_Production_WIP_history_{pro_num};
            """
            # INSERT new records into DB
            try:
                insert_start = dt.now()
                # Establish DB Connections
                db_conn = make_connection(SERVER_NAME_sbi, DATABASE_NAME_sbi)

                with db_conn.cursor() as cursor:
                    upload_size = len(unravel_dfs)

                    # SQL UPLOAD
                    # Flags for upload process progress
                    nickel = dime = dime_2 = quarter = dime_3 = dime_4 = half = dime_6 = quarter_3 = \
                        dime_8 = ninety = ninety_5 = True
                    progress_prompt = f"\n({pro_num}) {'SR' if isServer else 'RE'} WIP INSERT operation at "

                    print(f"\n({pro_num}) {'SR' if isServer else 'RE'} WIP INSERT operation is "
                          f"running on the background. Progress will show intermittently\n")
                    # Create temp table
                    cursor.execute(create_query_temp)

                    if pro_num == 2:  # SQL upload for SR data - Process 2
                        for index, wip_item in enumerate(tqdm(unravel_dfs, total=upload_size,
                                                              desc=f"INSERTING new ({pro_num}) "
                                                                   f"{'SR' if isServer else 'RE'} "
                                                                   f"WIP records in chunks")):
                            cursor.execute(insert_query_temp.format(pro_num,
                                                                    items_to_SQL_values(wip_item, isForUpdate=False,
                                                                                        chunk_size=
                                                                                        (len(wip_item) // num_cols))))
                            # Bulk INSERT
                            if (index + 1) == upload_size:
                                print(f"\n({pro_num}) {'SR' if isServer else 'RE'} - SEMAPHORE WARNING: YELLOW")
                                ti.sleep(7.3)
                                with semaphore:
                                    print(f"\n({pro_num}) {'SR' if isServer else 'RE'} "
                                          f"BULK INSERT - SEMAPHORE WARNING: RED {dt.now()}")
                                    cursor.execute(insert_query_main)
                                    print(f"\nSEMAPHORE GREEN for Process #{pro_num}")
                                cursor.execute(drop_query_temp)  # End of BULK INSERT
                                print(f"\n({pro_num}) DROPPED temp {'SR' if isServer else 'RE'} WIP table")

                    else:  # SQL upload for RE data and SR data - Processes 1, 3, 4
                        for index, wip_item in enumerate(unravel_dfs):
                            cursor.execute(insert_query_temp.format(pro_num,
                                                                    items_to_SQL_values(wip_item,
                                                                                        isForUpdate=False,
                                                                                        chunk_size=
                                                                                        (len(wip_item) // num_cols))))
                            # Bulk INSERT
                            if (index + 1) == upload_size:
                                print(f"\n({pro_num}) {'SR' if isServer else 'RE'} - SEMAPHORE WARNING: YELLOW")
                                ti.sleep(7.3)
                                with semaphore:
                                    print(f"\n({pro_num}) {'SR' if isServer else 'RE'} "
                                          f"BULK INSERT - SEMAPHORE WARNING: RED {dt.now()}")
                                    cursor.execute(insert_query_main)
                                    print(f"\nSEMAPHORE GREEN for Process #{pro_num}")
                                cursor.execute(drop_query_temp)  # End of BULK INSERT
                                print(f"\n({pro_num}) DROPPED temp {'SR' if isServer else 'RE'} WIP table")

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
            except Exception as e:
                print(repr(e))
                LOGGER.error(Messages.SQL_I_ERROR.value, exc_info=True)
                show_message(AlertType.FAILED)
                sys.exit()
            else:
                db_conn.commit()
                print(f"\n({pro_num}) {'SR' if isServer else 'RE'} {Messages.SUCCESS_LOAD_OP.value}.\n"
                      f"T: {dt.now() - insert_start}\n")
                # Close the DB connection
                db_conn.close()
        # If no new records in server_dw_list
        else:
            print("\nNo new records to INSERT\n")
