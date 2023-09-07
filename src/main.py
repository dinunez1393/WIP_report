from transform import *
from db_conn import *
from alerts import *
from extraction import select_wip_maxStatus
from loading import *
from update import *
from delete import delete_oldData
import warnings
import pandas as pd
from utilities import *
import asyncio
import logging
import threading


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
    print("WIP ANALYSIS\n\n\n_______________________________________________________________________________________")
    program_start = dt.now()
    try:
        # Establish DB Connections
        conn_sbi = make_connection(SERVER_NAME_sbi, DATABASE_NAME_sbi)
        # Supress all warning messages
        warnings.simplefilter("ignore")

        # Delete oldest WIP data (older than 185 days)
        delete_oldData(conn_sbi)

        # Get all the raw data
        re_rawData_df, sr_rawData_df = asyncio.run(initializer(conn_sbi))
        # Get the latest WIP status to continue counting WIP from there
        latest_wip_status_df = select_wip_maxStatus(conn_sbi, isForUpdate=False)

        # Clean the data in threads and make a WIP report
        result_storage = ['dummy_1', 'dummy_2']
        thread_sr = threading.Thread(target=assign_wip, args=(sr_rawData_df, latest_wip_status_df.copy(),
                                                              result_storage))
        thread_re = threading.Thread(target=assign_wip, args=(re_rawData_df, latest_wip_status_df.copy(),
                                                              result_storage, False))
        thread_sr.start()
        thread_re.start()
        thread_sr.join()
        thread_re.join()

        # Upload the cleaned data
        sr_wip_df = result_storage[0]
        re_wip_df = result_storage[1]
        load_wip_data(conn_sbi, sr_wip_df, to_csv=True)
        load_wip_data(conn_sbi, re_wip_df, to_csv=True, isServer=False)

        # Update the shipment status from WIP table
        wip_shipped_df, wip_stillNotShipped_df = assign_shipmentStatus(conn_sbi)

        # Post the update to SQL
        update_wip_data(conn_sbi, [wip_shipped_df, wip_stillNotShipped_df], to_csv=True)
        load_wip_data(conn_sbi, wip_stillNotShipped_df, to_csv=True)
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
