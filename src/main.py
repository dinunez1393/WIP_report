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
import logging
import multiprocessing


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

        # Given that the SR dataframe is very large, it is split into three, so that it feeds three cleaning processes
        splitting_start = dt.now()
        print("Splitting the SR dataframe in the background...")
        sr_rawData_df_1, sr_rawData_df_2, sr_rawData_df_3, sr_rawData_cats_1, sr_rawData_cats_2, sr_rawData_cats_3 = \
            df_splitter(sr_rawData_df)
        print(f"SR dataframe split complete. T: {dt.now() - splitting_start}")
        splitting_start = dt.now()
        print("Splitting the SR SAP dataframe in the background...")
        sr_sap_statusH_df_1 = sr_sap_statusH_df[sr_sap_statusH_df['SerialNumber'].isin(sr_rawData_cats_1)]
        sr_sap_statusH_df_2 = sr_sap_statusH_df[sr_sap_statusH_df['SerialNumber'].isin(sr_rawData_cats_2)]
        sr_sap_statusH_df_3 = sr_sap_statusH_df[sr_sap_statusH_df['SerialNumber'].isin(sr_rawData_cats_3)]
        print(f"SR SAP dataframe split complete. T: {dt.now() - splitting_start}\n")

        # Clean the data in multiple processes and make a WIP report
        lock = multiprocessing.Lock()
        process_1_sr = multiprocessing.Process(target=assign_wip,
                                               args=(sr_rawData_df_1.copy(), sr_sap_statusH_df_1.copy(), lock),
                                               name="SR_Pro_1")
        process_2_sr = multiprocessing.Process(target=assign_wip,
                                               args=(sr_rawData_df_2.copy(), sr_sap_statusH_df_2.copy(), lock),
                                               name="SR_Pro_2")
        process_3_sr = multiprocessing.Process(target=assign_wip,
                                               args=(sr_rawData_df_3.copy(), sr_sap_statusH_df_3.copy(), lock),
                                               name="SR_Pro_3")
        process_re = multiprocessing.Process(target=assign_wip, args=(re_rawData_df.copy(), re_sap_statusH_df.copy(),
                                                                      lock, False), name="RE_Pro")
        process_1_sr.start()
        process_2_sr.start()
        process_3_sr.start()
        process_re.start()

        process_1_sr.join()
        process_2_sr.join()
        process_3_sr.join()
        process_re.join()

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
