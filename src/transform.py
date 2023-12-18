# Functions for raw data transformations
import pandas as pd
from model import *
from extraction import *
from utilities import *
from loading import *
from tqdm import tqdm
import asyncio
import logging
from datetime import datetime as dt
import multiprocessing


async def get_raw_data(async_pool_asbuilt, conn_sbi):
    """
    Function that uses the SQL extraction functions to extract raw data and distribute the data to be ready for cleaning
    Note: The data is not totally raw. It has passed some processing by merging.
    :param async_pool_asbuilt: Asynchronous pool for Asbuilt DB
    # :param async_pool_sbi: Asynchronous pool for SBI DB
    :param conn_sbi: The connection for SBI DB
    :return: A tuple containing four dataframes for rack and server raw data
    :rtype: tuple
    """

    extraction_start = dt.now()
    # Run the select queries
    print("\nSELECT queries running concurrently in the background...\n")
    wip_maxDate = datetime_from_py_to_sql(select_wip_maxDate(conn_sbi))
    results = await asyncio.gather(select_ph_rawData(async_pool_asbuilt, wip_maxDate),
                                   select_sap_historicalStatus(async_pool_asbuilt, wip_maxDate),
                                   select_customers(async_pool_asbuilt))
    print(f"\nTOTAL extraction time: {dt.now() - extraction_start}")

    allocation_start = dt.now()
    print("Data allocation is running in the background...")

    rackBuild_ckps = {200, 235, 236, 254, 255, 208, 209, 252, 253, 201}
    eol_ckps = {216, 217, 218, 219, 260, 2470, 247}

    re_rawData_df, sr_rawData_df = results[0]
    sr_sap_statusH_df, re_sap_statusH_df = results[1]
    customers_df = results[2]
    # srSNs_with_flags_df, reSNs_with_flags_df = results[3]  # Currently inactive  # FIXME: Possibly obsolete

    # Purge Rack Build, End-of-Line and Rack Hi-Pot data that might be in server data to avoid having
    # duplicates further on
    mask = ~sr_rawData_df['CheckPointId'].isin(rackBuild_ckps.union(eol_ckps))
    sr_rawData_df = sr_rawData_df[mask]

    # Label the unit type: Server or rack
    sr_rawData_df['ProductType'] = "Server"
    re_rawData_df['ProductType'] = "Rack"

    # Drop unnecessary duplicates
    sr_rawData_df = sr_rawData_df.drop_duplicates(subset=['TransID'])
    re_rawData_df = re_rawData_df.drop_duplicates(subset=['TransID'])

    # Assign customers
    sr_rawData_df = sr_rawData_df.merge(customers_df, how='left', on='StockCode')
    re_rawData_df = re_rawData_df.merge(customers_df, how='left', on='StockCode')

    print(f"\nTOTAL raw data allocation time: {dt.now() - allocation_start}")

    return re_rawData_df, sr_rawData_df, sr_sap_statusH_df, re_sap_statusH_df  # , srSNs_with_flags_df, reSNs_with_flags_df - FIXME


def assign_wip(semaphore, isServerLevel=True):
    """
    Function that cleans the processed raw data to make a full WIP report and uses another function to
     export the cleaned data to SQL or CSV
    :param semaphore: Semaphore for concurrent loading processes
    :type semaphore: multiprocessing.Semaphore
    :param isServerLevel: a flag that indicates whether the raw data is server data or rack data
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

    # Import raw data
    import_start = dt.now()
    print(f"({pro_num}) Importing raw data\n")
    rawData_df = pd.read_hdf(f"../CleanedRecords_csv/wip_rawData_p{pro_num}.h5", key='data')
    sap_historicalStatus_df = pd.read_hdf(f"../CleanedRecords_csv/sap_historyData_p{pro_num}.h5", key='data')
    print(f"({pro_num}) Raw data import is complete. T: {dt.now() - import_start}")

    PARTITION_SIZE = 300_000
    cleaning_start = dt.now()
    unit_todayNow = dt.now()
    counter = 0
    wip_list = []
    master_list = []
    areas = {'Server Build': [100, 101],
             'Rack Build': [200, 235, 254, 208, 252],
             'System Test': [150, 170],
             'End of Line': [216, 218, 260, 202, 243, 2470, 228, 270, 237, 230, 300, 301, 1510, 234, 302]}
    wip_columns = ['Site', 'Building', 'SerialNumber', 'StockCode', 'StockCodePrefix', 'SKU', 'CheckPointId',  # TODO: Add the new columns
                   'CheckPointName', 'Area', 'TransID', 'TransactionDate', 'WIP_SnapshotTime', 'SnapshotTime',
                   'DwellTime_calendar', 'DwellTime_working', 'OrderType', 'FactoryStatus',
                   'ProductType', 'Customer', 'PackedIsLast_flag', 'PackedPreviously_flag',
                   'VoidSN_Previously_flag', 'ReworkScanPreviously_flag',
                   'ETL_time']  # 'isFrom_WIP' - Disabled indefinitely
    distinctSN_count = rawData_df['SerialNumber'].nunique()

    # Reindex the raw data and concatenate with existing data from WIP table
    rawData_df = rawData_df.reindex(columns=wip_columns)

    # Group the product history raw data and the SAP historical status data by Serial Number
    ph_instances_grouped = rawData_df.groupby('SerialNumber')
    sap_statusH_grouped = sap_historicalStatus_df.groupby('SerialNumber')

    # Flags for process progress
    nickel = dime = dime_2 = quarter = dime_3 = dime_4 = half = dime_6 = quarter_3 = dime_8 = ninety = ninety_5 = True
    progress_prompt = f"\n({pro_num}) {'SR' if isServerLevel else 'RE'} WIP cleaning operation at "

    # Cleaning
    if pro_num == 2:  # Cleaning for Server Level WIP - Process 2
        for serialNumber, ph_instance_df in tqdm(ph_instances_grouped, desc="(2) SR WIP cleaning operation progress"):
            # Get the SAP historical status data for this serial number
            try:
                sap_historicalStatus_df = sap_statusH_grouped.get_group(serialNumber).reset_index(drop=True)
            except KeyError:
                sap_historicalStatus_df = None

            cleaned_wip = UnitHistory(ph_instance_df.reset_index(drop=True), sap_historicalStatus_df, unit_todayNow)
            wip_list.extend(cleaned_wip.determine_processAndArea())
            if len(wip_list) > PARTITION_SIZE:
                master_list.append(wip_list.copy())
                wip_list.clear()
    else:  # Cleaning for Rack Level WIP and Server Level WIP - Processes 1 & 3
        print(f"({pro_num}) {'SR' if isServerLevel else 'RE'} WIP cleaning operation is running on the background. "
              f"Progress will show intermittently")
        for serialNumber, ph_instance_df in ph_instances_grouped:
            # Get the SAP historical status data for this serial number
            try:
                sap_historicalStatus_df = sap_statusH_grouped.get_group(serialNumber).reset_index(drop=True)
            except KeyError:
                sap_historicalStatus_df = None

            cleaned_wip = UnitHistory(ph_instance_df.reset_index(drop=True), sap_historicalStatus_df, unit_todayNow,
                                      isServerLevel)
            wip_list.extend(cleaned_wip.determine_processAndArea())
            if len(wip_list) > PARTITION_SIZE:
                master_list.append(wip_list.copy())
                wip_list.clear()

            # Provide loop progress feedback for 5%, 10%, 20%, 25%, 30%, 40%, 50%, 60%, 75%, 80%, 90%, and 95%
            counter += 1
            current_progress = counter / distinctSN_count
            if ninety_5 and current_progress >= 0.95:
                print(f"{progress_prompt}95% ({distinctSN_count} items to clean) T: {dt.now() - cleaning_start}")
                ninety_5 = False
            elif ninety and current_progress >= 0.9:
                print(f"{progress_prompt}90% ({distinctSN_count} items to clean) T: {dt.now() - cleaning_start}")
                ninety = False
            elif dime_8 and current_progress >= 0.8:
                print(f"{progress_prompt}80% ({distinctSN_count} items to clean) T: {dt.now() - cleaning_start}")
                dime_8 = False
            elif quarter_3 and current_progress >= 0.75:
                print(f"{progress_prompt}75% ({distinctSN_count} items to clean) T: {dt.now() - cleaning_start}")
                quarter_3 = False
            elif dime_6 and current_progress >= 0.6:
                print(f"{progress_prompt}60% ({distinctSN_count} items to clean) T: {dt.now() - cleaning_start}")
                dime_6 = False
            elif half and current_progress >= 0.5:
                print(f"{progress_prompt}50% ({distinctSN_count} items to clean) T: {dt.now() - cleaning_start}")
                half = False
            elif dime_4 and current_progress >= 0.4:
                print(f"{progress_prompt}40% ({distinctSN_count} items to clean) T: {dt.now() - cleaning_start}")
                dime_4 = False
            elif dime_3 and current_progress >= 0.3:
                print(f"{progress_prompt}30% ({distinctSN_count} items to clean) T: {dt.now() - cleaning_start}")
                dime_3 = False
            elif quarter and current_progress >= 0.25:
                print(f"{progress_prompt}25% ({distinctSN_count} items to clean) T: {dt.now() - cleaning_start}")
                quarter = False
            elif dime_2 and current_progress >= 0.2:
                print(f"{progress_prompt}20% ({distinctSN_count} items to clean) T: {dt.now() - cleaning_start}")
                dime_2 = False
            elif dime and current_progress >= 0.1:
                print(f"{progress_prompt}10% ({distinctSN_count} items to clean) T: {dt.now() - cleaning_start}")
                dime = False
            elif nickel and current_progress >= 0.05:
                print(f"{progress_prompt}5% ({distinctSN_count} items to clean) T: {dt.now() - cleaning_start}")
                nickel = False
        print(f"{progress_prompt}100%. Duration: {dt.now() - cleaning_start}\n")

    # Convert WIP list to a dataframe
    allocation_start = dt.now()
    if 0 < len(wip_list) <= PARTITION_SIZE:  # Still append for wip_list less than 300K
        master_list.append(wip_list.copy())
        wip_list.clear()
    wip_dfs_list = []

    print(f"\nAllocating the cleaned ({pro_num}){'SR' if isServerLevel else 'RE'} WIP data - "
          f"{len(master_list)} items:\n")
    for index, wip_list in enumerate(master_list):
        time_tracker = dt.now()
        wip_df = pd.DataFrame(wip_list, columns=wip_columns)

        print(f"({index + 1}) Initialized the ({pro_num}){'SR' if isServerLevel else 'RE'} "
              f"WIP dataframe from the tuples. T: {dt.now() - time_tracker}")

        # Datetime to string conversion
        if wip_df.shape[0] > 0:
            # Dwell time calculations
            print(f"{wip_df.shape[0]:,} items in ({pro_num}){'SR' if isServerLevel else 'RE'} "
                  f"WIP dataframe ({index + 1})")
            time_tracker = dt.now()
            wip_df['DwellTime_calendar'] = wip_df['WIP_SnapshotTime'] - wip_df['TransactionDate']
            wip_df['DwellTime_calendar'] = wip_df['DwellTime_calendar'].dt.total_seconds()
            wip_df['DwellTime_calendar'] /= 3600
            print(f"({index + 1}) ({pro_num}){'SR' if isServerLevel else 'RE'} "
                  f"WIP: Calendar dwell time calculations complete. T: {dt.now() - time_tracker}")
            time_tracker = dt.now()
            wip_df['DwellTime_working'] = wip_df.apply(lambda row: delta_working_hours(row['TransactionDate'],
                                                                                       row['WIP_SnapshotTime'],
                                                                                       calendar=False), axis=1)
            print(f"({index + 1}) ({pro_num}){'SR' if isServerLevel else 'RE'} "
                  f"WIP: Working time dwell time calculations complete. T: {dt.now() - time_tracker}")

            # Assign the process areas
            wip_df['Area'] = wip_df['Area'].astype(object)  # Explicitly cast column before assignment (pandas r.)
            for area, checkpoint_ids in tqdm(areas.items(), total=len(areas),
                                             desc=f"({index + 1}) Assigning the "
                                                  f"({pro_num}){'SR' if isServerLevel else 'RE'} process areas"):
                wip_df.loc[wip_df['CheckPointId'].isin(checkpoint_ids), 'Area'] = area

            # Convert python Datetime(s) to SQL Datetime
            time_tracker = dt.now()
            wip_df[['TransactionDate', 'WIP_SnapshotTime']] = wip_df[['TransactionDate', 'WIP_SnapshotTime']].applymap(
                datetime_from_py_to_sql)
            wip_df['ETL_time'] = datetime_from_py_to_sql(dt.now())
            print(f"({index + 1}) ({pro_num}){'SR' if isServerLevel else 'RE'} "
                  f"WIP: Datetime conversions to string complete. T: {dt.now() - time_tracker}")

            wip_dfs_list.append(wip_df.copy())

    if len(wip_dfs_list) < 1:
        final_wip_df = pd.DataFrame([], columns=wip_columns)  # Dummy DF to avoid producing an error
    else:
        print(f"Concatenating the ({pro_num}){'SR' if isServerLevel else 'RE'} dataframes in the background...")
        final_wip_df = pd.concat(wip_dfs_list, ignore_index=True)
    print(f"Cleaned ({pro_num}){'SR' if isServerLevel else 'RE'} WIP data allocation completed successfully in "
          f"{dt.now() - allocation_start}\n")

    # Load results
    load_wip_data(final_wip_df, semaphore, to_csv=False, isServer=isServerLevel)


# OBSOLETE:
def assign_shipmentStatus(db_conn):
    """
    Function gets all units that did not have shipment status and updates the current dwell time. If a new
    shipment status exists since the latest INSERT run, then the NotShipped flag is updated accordingly
    :param db_conn: The connection to the database
    :return: a dataframe with the latest shipment information updated
    :rtype: pandas.DataFrame
    """
    criticalShipment_ckps = {300, 301, 302}
    today_upperBoundary = fixed_date(select_wip_maxDate(db_conn, snapshotTime=True))

    wip_df = select_wip_maxStatus(db_conn)
    wip_df = wip_df.drop(columns=['LatestUpdateDate'])
    wip_df_columns = wip_df.columns.tolist()

    wip_grouped = wip_df.groupby('SerialNumber')

    wip_shipped_SNs = set()
    wip_stillNotShipped_tuples = []

    # Find the units that do not have Shipping Scan (for rack shipment) nor Carton Scan (for single servers)
    for serialNumber, wipHistory_df in tqdm(wip_grouped, desc="Assigning Shipment Status"):
        wipHistory_df = wipHistory_df.sort_values('WIP_SnapshotDate', ascending=False)
        max_ckp = wipHistory_df['CheckpointID'].iloc[0]
        max_timestamp = wipHistory_df['TransactionDate'].max(skipna=True)
        instance_upper_boundary = wipHistory_df['WIP_SnapshotDate'].max(skipna=True)

        if max_ckp in criticalShipment_ckps:
            wip_shipped_SNs.add(serialNumber)
        else:
            # Add WIP for all days from the latest instance timestamp to current date (today)
            current_date = instance_upper_boundary + timedelta(days=1)
            while current_date <= today_upperBoundary:
                if current_date < max_timestamp:
                    current_date = current_date + timedelta(days=1)
                    continue

                wipHistory_df['WIP_SnapshotDate'] = current_date
                wipHistory_df['DwellTime_calendar'] = delta_working_hours(max_timestamp, current_date)
                wipHistory_df['DwellTime_working'] = delta_working_hours(max_timestamp, current_date, calendar=False)
                max_row_tuple = tuple(wipHistory_df.values[0])  # Convert to tuple for faster loading of data
                wip_stillNotShipped_tuples.append(max_row_tuple)
                current_date = current_date + timedelta(days=1)

    # Create the WIP shipped dataframe
    wip_shipped_df = pd.DataFrame(wip_shipped_SNs, columns=['SerialNumber'])

    # Create the WIP not-shipped dataframe
    wip_stillNotShipped_df = pd.DataFrame(wip_stillNotShipped_tuples, columns=wip_df_columns)
    wip_stillNotShipped_df['ExtractionDate'] = datetime_from_py_to_sql(dt.now())
    wip_stillNotShipped_df['TransactionDate'] = \
        wip_stillNotShipped_df['TransactionDate'].apply(datetime_from_py_to_sql)
    wip_stillNotShipped_df['WIP_SnapshotDate'] = \
        wip_stillNotShipped_df['WIP_SnapshotDate'].apply(datetime_from_py_to_sql)

    unshipped_toUpdate_df = pd.DataFrame(set(wip_stillNotShipped_df['SerialNumber']), columns=['SerialNumber'])

    return wip_shipped_df, unshipped_toUpdate_df, wip_stillNotShipped_df
