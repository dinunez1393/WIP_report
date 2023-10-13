from transform import *
from db_conn import *
from alerts import *
from extraction import select_wip_maxStatus
from loading import *
from update import *
from delete import *
from datetime import datetime as dt
import warnings
import time as ti
import pandas as pd
from utilities import *
import asyncio
import logging
import threading
import gc


SERVER_NAME_sbi = 'WQMSDEV01'
DATABASE_NAME_sbi = 'SBILearning'
SERVER_NAME_asbuilt = 'ZwhirlpoolR'
DATABASE_NAME_asbuilt = 'ASBuiltDW'

GENERIC_ERROR = "An error occurred and the operation did not complete. Please check log."
RUN_T_ERROR = "A runtime error occurred"
ASYNC_ERROR = "An asynchronous runtime error occurred"
PROGRAM_END = "End of program execution"
SUCCESS_OP = "The operation completed successfully"

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.ERROR)


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
        LOGGER.error(GENERIC_ERROR, exc_info=True)
        show_message(AlertType.FAILED)
    else:
        print("ASYNC DB Connections ran successfully\n")
        # Close all DB connections
        async_pool_asbuilt.close()
        await async_pool_asbuilt.wait_closed()

        return all_rawData


if __name__ == '__main__':
    print(f"WIP ANALYSIS\n"
          f"({dt.now()})\n\n_______________________________________________________________________________________")
    program_start = dt.now()
    try:
        # Establish DB Connections
        conn_sbi = make_connection(SERVER_NAME_sbi, DATABASE_NAME_sbi)
        # Supress all warning messages
        warnings.simplefilter("ignore")

        # Clear WIP table for fresh upload
        delete_allData(conn_sbi)

        # Get all the raw data
        re_rawData_df, sr_rawData_df, sr_sap_statusH_df, re_sap_statusH_df = asyncio.run(initializer(conn_sbi))

        # Given that the SR dataframe is very large, it is split into two, so that it feeds two cleaning threads
        splitting_start = dt.now()
        print("Splitting the SR dataframe in the background...")
        sr_rawData_df_1, sr_rawData_df_2, sr_rawData_cats_1, sr_rawData_cats_2 = df_splitter(sr_rawData_df)
        print(f"SR dataframe split complete. T: {dt.now() - splitting_start}")
        splitting_start = dt.now()
        print("Splitting the SR SAP dataframe in the background...")
        sr_sap_statusH_df_1 = sr_sap_statusH_df[sr_sap_statusH_df['SerialNumber'].isin(sr_rawData_cats_1)]
        sr_sap_statusH_df_2 = sr_sap_statusH_df[sr_sap_statusH_df['SerialNumber'].isin(sr_rawData_cats_2)]
        print(f"SR SAP dataframe split complete. T: {dt.now() - splitting_start}\n")

        # De-allocate memory for unreferenced data structures at this point
        del sr_rawData_df, sr_sap_statusH_df
        gc.collect()

        # Clean the data in threads and make a WIP report
        lock = threading.Lock()
        thread_1_sr = threading.Thread(target=assign_wip, args=(sr_rawData_df_1, sr_sap_statusH_df_1, lock, conn_sbi),
                                       name="SR_Thr_1")
        thread_2_sr = threading.Thread(target=assign_wip, args=(sr_rawData_df_2, sr_sap_statusH_df_2, lock, conn_sbi),
                                       name="SR_Thr_2")
        thread_re = threading.Thread(target=assign_wip, args=(re_rawData_df, re_sap_statusH_df, lock, conn_sbi, False),
                                     name="RE_Thr_1")
        thread_1_sr.start()
        thread_2_sr.start()
        thread_re.start()

        # De-allocate memory for unreferenced data structures well beyond their reference point
        print(f"Memory de-allocation in 10 minutes from now: {dt.now()}")
        ti.sleep(601)
        del sr_rawData_df_1, sr_rawData_df_2, sr_sap_statusH_df_1, sr_sap_statusH_df_2, re_rawData_df, re_sap_statusH_df
        gc.collect()

        thread_1_sr.join()
        thread_2_sr.join()
        thread_re.join()

        # Update order type and factory status NULL values
        update_orderType_factoryStatus(conn_sbi)
    except Exception as e:
        print(repr(e))
        LOGGER.error(GENERIC_ERROR, exc_info=True)
        show_message(AlertType.FAILED)
    else:
        print("DB Connection ran successfully\n")
        # Close all DB connections
        conn_sbi.close()
        show_goodbye()
        print(f"Total program duration: {dt.now() - program_start}")
        show_message(AlertType.SUCCESS)
