from transform import *
from db_conn import *
from alerts import *
from extraction import select_wip_maxStatus
from loading import *
from update import *
from delete import *
from datetime import datetime as dt
import warnings
import pandas as pd
from utilities import *
import asyncio
import multiprocessing
import sys
import os
import json
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path


SERVER_NAME_sbi = 'WQMSDEV01'
DATABASE_NAME_sbi = 'SBILearning'
SERVER_NAME_asbuilt = 'ZwhirlpoolR'
DATABASE_NAME_asbuilt = 'ASBuiltDW'

LOGGER = logger_creator('RunTime_Error')


async def initializer(connection_sbi):
    """
    Function that initializes the raw data
    :param connection_sbi: the DB connection to SBI
    :return: tuple
    """
    try:
        # Establish ASYNC DB Connections
        async_pool_asbuilt = await create_async_pool(SERVER_NAME_asbuilt, DATABASE_NAME_asbuilt)
        # Supress all warning messages
        warnings.simplefilter("ignore")

        # Get all the raw data
        all_rawData = await get_raw_data(async_pool_asbuilt, connection_sbi)
    except Exception as err:
        print(repr(err))
        LOGGER.error(Messages.GENERIC_ERROR.value, exc_info=True)
        show_message(AlertType.FAILED)
        sys.exit()
    else:
        print("ASYNC DB Connections ran successfully\n")
        # Close all DB connections
        async_pool_asbuilt.close()
        await async_pool_asbuilt.wait_closed()

        return all_rawData


if __name__ == '__main__':
    pqFiles_folder = rf"{Path(__file__).parent.parent}{os.sep}CleanedRecords_csv{os.sep}"
    programMetaData_path = rf"{Path(__file__).parent}{os.sep}meta_data.json"

    print(f"WIP ANALYSIS\n"
          f"({dt.now()})\n\n_______________________________________________________________________________________")
    program_start = dt.now()
    try:
        # Establish DB Connections
        conn_sbi = make_connection(SERVER_NAME_sbi, DATABASE_NAME_sbi)
        # Supress all warning messages
        warnings.simplefilter("ignore")

        # Check the WIP semaphore
        with open(programMetaData_path, 'r') as meta_data_file:
            meta_data = json.load(meta_data_file)
            program_semaphore = meta_data['program_semaphore']

        if program_semaphore == 0:  # Perform raw data extraction
            # Clear WIP table old records
            delete_oldData(conn_sbi)

            # Get all the raw data
            re_rawData_df, sr_rawData_df, sr_sap_statusH_df, re_sap_statusH_df = asyncio.run(initializer(conn_sbi))

            # Given that the SR dataframe is very large, it is split into four, so that it feeds four cleaning processes
            splitting_start = dt.now()
            print("Splitting the SR dataframe in the background...")
            sr_rawData_df_1, sr_rawData_df_2, sr_rawData_df_3, sr_rawData_df_4, sr_rawData_cats_1, sr_rawData_cats_2, \
                sr_rawData_cats_3, sr_rawData_cats_4 = df_splitter(sr_rawData_df)
            print(f"SR dataframe split complete. T: {dt.now() - splitting_start}")
            splitting_start = dt.now()
            print("Splitting the SR SAP dataframe in the background...")
            sr_sap_statusH_df_1 = sr_sap_statusH_df[sr_sap_statusH_df['SerialNumber'].isin(sr_rawData_cats_1)]
            sr_sap_statusH_df_2 = sr_sap_statusH_df[sr_sap_statusH_df['SerialNumber'].isin(sr_rawData_cats_2)]
            sr_sap_statusH_df_3 = sr_sap_statusH_df[sr_sap_statusH_df['SerialNumber'].isin(sr_rawData_cats_3)]
            sr_sap_statusH_df_4 = sr_sap_statusH_df[sr_sap_statusH_df['SerialNumber'].isin(sr_rawData_cats_4)]
            print(f"SR SAP dataframe split complete. T: {dt.now() - splitting_start}\n")

            # Export raw data to PARQUET
            export_start = dt.now()
            print("Exporting raw data to PARQUET\n")

            srRaw_table1 = pa.Table.from_pandas(sr_rawData_df_1)
            srRaw_table2 = pa.Table.from_pandas(sr_rawData_df_2)
            srRaw_table3 = pa.Table.from_pandas(sr_rawData_df_3)
            srRaw_table4 = pa.Table.from_pandas(sr_rawData_df_4)
            reRaw_table = pa.Table.from_pandas(re_rawData_df)
            sr_sap_table1 = pa.Table.from_pandas(sr_sap_statusH_df_1)
            sr_sap_table2 = pa.Table.from_pandas(sr_sap_statusH_df_2)
            sr_sap_table3 = pa.Table.from_pandas(sr_sap_statusH_df_3)
            sr_sap_table4 = pa.Table.from_pandas(sr_sap_statusH_df_4)
            re_sap_table = pa.Table.from_pandas(re_sap_statusH_df)

            pq.write_table(srRaw_table1, rf"{pqFiles_folder}wip_rawData_p1.parquet")
            pq.write_table(srRaw_table2, rf"{pqFiles_folder}wip_rawData_p2.parquet")
            pq.write_table(srRaw_table3, rf"{pqFiles_folder}wip_rawData_p3.parquet")
            pq.write_table(srRaw_table4, rf"{pqFiles_folder}wip_rawData_p4.parquet")
            pq.write_table(reRaw_table, rf"{pqFiles_folder}wip_rawData_p5.parquet")
            pq.write_table(sr_sap_table1, rf"{pqFiles_folder}sap_historyData_p1.parquet")
            pq.write_table(sr_sap_table2, rf"{pqFiles_folder}sap_historyData_p2.parquet")
            pq.write_table(sr_sap_table3, rf"{pqFiles_folder}sap_historyData_p3.parquet")
            pq.write_table(sr_sap_table4, rf"{pqFiles_folder}sap_historyData_p4.parquet")
            pq.write_table(re_sap_table, rf"{pqFiles_folder}sap_historyData_p5.parquet")
            print(f"Export complete. T: {dt.now() - export_start}")

            # Update the WIP semaphore
            with open(programMetaData_path, 'w') as meta_data_file:
                meta_data['program_semaphore'] = 1
                json.dump(meta_data, meta_data_file)

            # Close all DB connections
            conn_sbi.close()
        else:  # Perform transformation and loading
            # Clean the data in multiple processes and make a WIP report
            semaphore = multiprocessing.Semaphore(1)  # Semaphore with counter value of 1 for concurrent data upload

            # Import unshipped and shipped data from WIP table
            unship_lastStatus_server_df, unship_lastStatus_rack_df = select_wip_maxStatus(conn_sbi)
            ship_lastStatus_server_df, ship_lastStatus_rack_df = select_wip_maxStatus(conn_sbi, packed=True)

            with multiprocessing.Manager() as manager:
                unpacked_SNs = manager.list()

                process_1_sr = multiprocessing.Process(target=assign_wip, args=(semaphore, True,
                                                                                unship_lastStatus_server_df,
                                                                                ship_lastStatus_server_df,
                                                                                unpacked_SNs),
                                                       name="SR_Pro_1")

                process_2_sr = multiprocessing.Process(target=assign_wip,  args=(semaphore,), name="SR_Pro_2")
                process_3_sr = multiprocessing.Process(target=assign_wip, args=(semaphore,), name="SR_Pro_3")
                process_4_sr = multiprocessing.Process(target=assign_wip, args=(semaphore,), name="SR_Pro_4")

                process_re = multiprocessing.Process(target=assign_wip, args=(semaphore, False,
                                                                              unship_lastStatus_rack_df,
                                                                              ship_lastStatus_rack_df,
                                                                              unpacked_SNs),
                                                     name="RE_Pro")

                process_1_sr.start()
                process_2_sr.start()
                process_3_sr.start()
                process_4_sr.start()
                process_re.start()

                process_1_sr.join()
                process_2_sr.join()
                process_3_sr.join()
                process_4_sr.join()
                process_re.join()

                # Update order type and factory status NULL values
                update_str_nulls(conn_sbi)
                # Update shipment status flag (packed is last)
                update_shipmentFlag(conn_sbi, unpacked_SNs)

            # Update the WIP semaphore
            with open(programMetaData_path, 'w') as meta_data_file:
                meta_data['program_semaphore'] = 0
                json.dump(meta_data, meta_data_file)

            # Close all DB connections
            conn_sbi.close()
    except Exception as e:
        print(repr(e))
        LOGGER.error(Messages.GENERIC_ERROR.value, exc_info=True)
        show_message(AlertType.FAILED)

        # Update program success flag for failure
        with open(programMetaData_path, 'w') as meta_data_file:
            meta_data['program_success'] = 0
            json.dump(meta_data, meta_data_file)

        sys.exit()
    else:
        print("DB Connection ran successfully\n")
        show_goodbye()
        print(f"Program duration: {dt.now() - program_start}")

        # Update program success flag
        with open(programMetaData_path, 'w') as meta_data_file:
            meta_data['program_success'] = 1
            json.dump(meta_data, meta_data_file)
