# Functions for raw data transformations
import pandas as pd
from model import *
from extraction import *
from utilities import *
from tqdm import tqdm
import asyncio
from datetime import datetime as dt


async def get_raw_data(async_pool_asbuilt, conn_sbi):
    """
    Function that uses the SQL extraction functions to extract raw data and distribute the data to be ready for cleaning
    Note: The data is not totally raw. It has passed some processing by merging.
    :param async_pool_asbuilt: Asynchronous pool for Asbuilt DB
    :param conn_sbi: The connection for SBI DB
    :return: A tuple containing two dataframes for rack and server raw data
    :rtype: tuple
    """

    extraction_start = dt.now()
    # Run the select queries
    print("\nSELECT queries running concurrently in the background...\n")
    wip_maxDate = datetime_from_py_to_sql(select_wip_maxDate(conn_sbi))
    results = await asyncio.gather(select_ph_rawData(async_pool_asbuilt, wip_maxDate),
                                   select_ph_rackBuildData(async_pool_asbuilt, wip_maxDate),
                                   select_ph_rackEoL_data(async_pool_asbuilt, wip_maxDate))
    print(f"\nTOTAL extraction time: {dt.now() - extraction_start}")
    print("Data allocation is running in the background...")

    re_rawData_df = results[0][0]
    sr_rawData_df = results[0][1]
    re_rackBuild_df = results[1]
    re_rackEoL_df = results[2]

    # Purge End-of-Line and Rack Hi-Pot data that might be in server data to avoid having duplicates further on
    mask = ~sr_rawData_df['CheckPointId'].isin({216, 217, 218, 219, 260, 2470, 247})
    sr_rawData_df = sr_rawData_df[mask]

    # Merge rack build information for server:
    # Server assembly finish data
    sr_assemblyFinish_df = sr_rawData_df[sr_rawData_df['CheckPointId'] == 101]
    sr_assemblyFinish_df = sr_assemblyFinish_df[['SerialNumber', 'StockCode', 'TransactionDate', 'CheckPointId', 'SKU',
                                                 'OrderType', 'FactoryStatus']]
    sr_assemblyFinish_df = sr_assemblyFinish_df.rename(columns={'StockCode': 'StockCode_af',
                                                                'TransactionDate': 'TransactionDate_af',
                                                                'SKU': 'SKU_af',
                                                                'OrderType': 'OrderType_af',
                                                                'FactoryStatus': 'FactoryStatus_af'})

    # SLT check-in data
    sr_sltIn_df = sr_rawData_df[sr_rawData_df['CheckPointId'] == 150]
    sr_sltIn_df = sr_sltIn_df[['SerialNumber', 'StringField1', 'TransactionDate', 'CheckPointId']]
    sr_sltIn_df = sr_sltIn_df.rename(columns={'TransactionDate': 'TransactionDate_sltIn'})

    # Merge SLT-in, Server assembly finish, and rack build dataframes.
    # Then, drop rows whose rack build timestamps do not fall within server assembly finish and SLT-in timestamps
    sr_rackBuild_df = sr_sltIn_df.merge(re_rackBuild_df, left_on='StringField1', right_on='RackSN', how='inner')
    sr_rackBuild_df = sr_rackBuild_df.merge(sr_assemblyFinish_df, on='SerialNumber', how='inner')
    sr_rackBuild_df = sr_rackBuild_df[(sr_rackBuild_df['TransactionDate_af'] <= sr_rackBuild_df['TransactionDate']) &
                                      (sr_rackBuild_df['TransactionDate'] <= sr_rackBuild_df['TransactionDate_sltIn'])]
    # Create a new trans ID by adding the SN and 10 billion to the original TransID to make it unique.
    # Currently, it is not unique because it is using the RE TransID
    sr_rackBuild_df['int_SN'] = sr_rackBuild_df['SerialNumber'].apply(str_extract_digits)
    sr_rackBuild_df['TransID'] = sr_rackBuild_df['TransID'].add(10_000_000_000)
    sr_rackBuild_df['TransID'] = sr_rackBuild_df['TransID'] + sr_rackBuild_df['int_SN']
    # Move the SR stock code to the StockCode column, which currently has the RE-StockCode,
    # and RE-checkpoint ID to the checkpoint ID column, as well as SKU, OrderType, and FactoryStatus for server
    sr_rackBuild_df['StockCode'] = sr_rackBuild_df['StockCode_af']
    sr_rackBuild_df['CheckPointId'] = sr_rackBuild_df['CheckPointId_y'].astype(int)
    sr_rackBuild_df['SKU'] = sr_rackBuild_df['SKU_af']
    sr_rackBuild_df['OrderType'] = sr_rackBuild_df['OrderType_af']
    sr_rackBuild_df['FactoryStatus'] = sr_rackBuild_df['FactoryStatus_af']

    # Concatenate server rack build data to main server raw data and drop unnecessary columns
    sr_rawData_df = pd.concat([sr_rawData_df, sr_rackBuild_df], ignore_index=True)
    sr_rawData_df = sr_rawData_df.drop(columns=['TransactionDate_sltIn', 'CheckPointId_x', 'RackSN',
                                                'CheckPointId_y', 'StockCode_af', 'SKU_af', 'OrderType_af',
                                                'FactoryStatus_af', 'TransactionDate_af', 'int_SN'])

    # Merge Rack End of Line data for server:
    # Server SLT-pass data
    sr_sltPass_df = sr_rawData_df[sr_rawData_df['CheckPointId'] == 102]
    sr_sltPass_df = sr_sltPass_df[['SerialNumber', 'StockCode', 'TransactionDate', 'CheckPointId', 'SKU',
                                   'OrderType', 'FactoryStatus']]
    sr_sltPass_df = sr_sltPass_df.rename(columns={'StockCode': 'StockCode_sltP',
                                                  'TransactionDate': 'TransactionDate_sltP',
                                                  'SKU': 'SKU_sltP',
                                                  'OrderType': 'OrderType_sltP',
                                                  'FactoryStatus': 'FactoryStatus_sltP'})

    # Server rack scan 1 data
    sr_rackScan_df = sr_rawData_df[sr_rawData_df['CheckPointId'] == 202]
    sr_rackScan_df = sr_rackScan_df[['SerialNumber', 'StringField1', 'TransactionDate', 'CheckPointId']]
    sr_rackScan_df = sr_rackScan_df.rename(columns={'TransactionDate': 'TransactionDate_rs'})

    # Merge SLT-pass, server rack scan, and rack End-of-Line dataframes.
    # Then, drop rows whose rack End-of-Line timestamps do not fall within server SLT-pass and Rack Scan timestamps
    sr_rackEoL_df = sr_rackScan_df.merge(re_rackEoL_df, left_on='StringField1', right_on='RackSN', how='inner')
    sr_rackEoL_df = sr_rackEoL_df.merge(sr_sltPass_df, on='SerialNumber', how='inner')
    sr_rackEoL_df = sr_rackEoL_df[(sr_rackEoL_df['TransactionDate_sltP'] <= sr_rackEoL_df['TransactionDate']) &
                                  (sr_rackEoL_df['TransactionDate'] <= sr_rackEoL_df['TransactionDate_rs'])]
    # Create a new trans ID by adding the SN and 10 billion to the original TransID to make it unique.
    # Currently, it is not unique because it is using the RE TransID
    sr_rackEoL_df['int_SN'] = sr_rackEoL_df['SerialNumber'].apply(str_extract_digits)
    sr_rackEoL_df['TransID'] = sr_rackEoL_df['TransID'].add(10_000_000_000)
    sr_rackEoL_df['TransID'] = sr_rackEoL_df['TransID'] + sr_rackEoL_df['int_SN']
    # Move the SR stock code to the StockCode column, which currently has the RE-StockCode,
    # and RE-checkpoint ID to the checkpoint ID column, as well as SKU, OrderType, and FactoryStatus for server
    sr_rackEoL_df['StockCode'] = sr_rackEoL_df['StockCode_sltP']
    sr_rackEoL_df['CheckPointId'] = sr_rackEoL_df['CheckPointId_y'].astype(int)
    sr_rackEoL_df['SKU'] = sr_rackEoL_df['SKU_sltP']
    sr_rackEoL_df['OrderType'] = sr_rackEoL_df['OrderType_sltP']
    sr_rackEoL_df['FactoryStatus'] = sr_rackEoL_df['FactoryStatus_sltP']

    # Concatenate server rack End-of-Line data to main server raw data and drop unnecessary columns
    sr_rawData_df = pd.concat([sr_rawData_df, sr_rackEoL_df], ignore_index=True)
    sr_rawData_df = sr_rawData_df.drop(columns=['TransactionDate_rs', 'CheckPointId_x', 'RackSN',
                                                'CheckPointId_y', 'StockCode_sltP', 'SKU_sltP', 'OrderType_sltP',
                                                'FactoryStatus_sltP', 'TransactionDate_sltP', 'int_SN'])

    # Merge Rack Hi-Pot data for server. Note: Server data exists in rack hipot checkpoint, but it is not complete
    # Server Rack Test check-out data
    sr_rackTestOut_df = sr_rawData_df[sr_rawData_df['CheckPointId'] == 224]
    sr_rackTestOut_df = sr_rackTestOut_df[['SerialNumber', 'StringField1', 'StockCode', 'TransactionDate',
                                           'CheckPointId', 'SKU', 'OrderType', 'FactoryStatus']]
    sr_rackTestOut_df = sr_rackTestOut_df.rename(columns={'StringField1': 'RackSN',
                                                          'StockCode': 'StockCode_rt',
                                                          'TransactionDate': 'TransactionDate_rt',
                                                          'CheckPointId': 'CheckPointId_rt',
                                                          'SKU': 'SKU_rt',
                                                          'OrderType': 'OrderType_rt',
                                                          'FactoryStatus': 'FactoryStatus_rt'})

    # Server final touch check-in data
    sr_finalTouch_df = sr_rawData_df[sr_rawData_df['CheckPointId'] == 228]
    sr_finalTouch_df = sr_finalTouch_df[['SerialNumber', 'TransactionDate', 'CheckPointId']]
    sr_finalTouch_df = sr_finalTouch_df.rename(columns={'TransactionDate': 'TransactionDate_ft',
                                                        'CheckPointId': 'CheckPointId_ft'})

    # Rack Hi-Pot data
    re_hipot_df = re_rawData_df[(re_rawData_df['CheckPointId'] == 2470) | (re_rawData_df['CheckPointId'] == 247)]
    re_hipot_df = re_hipot_df.rename(columns={'SerialNumber': 'RackSN'})

    # Merge Rack Test check-out, final touch, and rack hi-pot dataframes.
    # Then, drop rows whose rack hi-pot timestamps do not fall within rack test check-out and final touch check-in
    sr_rackHipot_df = sr_rackTestOut_df.merge(re_hipot_df, on='RackSN', how='inner')
    sr_rackHipot_df = sr_rackHipot_df.merge(sr_finalTouch_df, on='SerialNumber', how='inner')
    sr_rackHipot_df = sr_rackHipot_df[(sr_rackHipot_df['TransactionDate_rt'] <= sr_rackHipot_df['TransactionDate']) &
                                      (sr_rackHipot_df['TransactionDate'] <= sr_rackHipot_df['TransactionDate_ft'])]
    # Create a new trans ID by adding the SN and 10 billion to the original TransID to make it unique.
    # Currently, it is not unique because it is using the RE TransID
    sr_rackHipot_df['int_SN'] = sr_rackHipot_df['SerialNumber'].apply(str_extract_digits)
    sr_rackHipot_df['TransID'] = sr_rackHipot_df['TransID'].add(10_000_000_000)
    sr_rackHipot_df['TransID'] = sr_rackHipot_df['TransID'] + sr_rackHipot_df['int_SN']
    # Move the SR stock code to the StockCode column, which currently has the RE-StockCode,
    # and RE-checkpoint ID to the checkpoint ID column, as well as RackSN, SKU, OrderType, and FactoryStatus for server
    sr_rackHipot_df['StockCode'] = sr_rackHipot_df['StockCode_rt']
    sr_rackHipot_df['CheckPointId'] = sr_rackHipot_df['CheckPointId'].astype(int)
    sr_rackHipot_df['StringField1'] = sr_rackHipot_df['RackSN']
    sr_rackHipot_df['SKU'] = sr_rackHipot_df['SKU_rt']
    sr_rackHipot_df['OrderType'] = sr_rackHipot_df['OrderType_rt']
    sr_rackHipot_df['FactoryStatus'] = sr_rackHipot_df['FactoryStatus_rt']

    # Concatenate rack hi-pot for server data to main server raw data and drop unnecessary columns
    sr_rawData_df = pd.concat([sr_rawData_df, sr_rackHipot_df], ignore_index=True)
    sr_rawData_df = sr_rawData_df.drop(columns=['TransactionDate_ft', 'CheckPointId_ft', 'CheckPointId_rt',
                                                'RackSN', 'StockCode_rt', 'SKU_rt', 'OrderType_rt',
                                                'FactoryStatus_rt', 'TransactionDate_rt', 'int_SN'])
    # Label the unit type: Server or rack
    sr_rawData_df['ProductType'] = "Server"
    re_rawData_df['ProductType'] = "Rack"

    # Drop unnecessary duplicates
    sr_rawData_df = sr_rawData_df.drop_duplicates(subset=['TransID'])
    re_rawData_df = re_rawData_df.drop_duplicates(subset=['TransID'])

    print(f"\nTOTAL raw data allocation time: {dt.now() - extraction_start}")

    return re_rawData_df, sr_rawData_df


def assign_wip(rawData_df, latest_wip_status_df, result_store, isServerLevel=True):
    """
    Function that cleans the processed raw data to make a full WIP report
    :param rawData_df: the dataframe containing the raw data
    :type rawData_df: pandas.Dataframe
    :param latest_wip_status_df: a dataframe containing the serial numbers in WIP table with their respective MAX
    snapshot time
    :type latest_wip_status_df: pandas.Dataframe
    :param result_store: A list object for storing the cleaned results
    :type result_store: list
    :param isServerLevel: a flag that indicates whether the raw data is server data or rack data
    :return: full WIP report
    :rtype: pandas.Dataframe
    """
    cleaning_start = dt.now()
    counter = 0
    wip_list = []
    master_list = []
    wip_columns = ['Site', 'Building', 'SerialNumber', 'StockCode', 'SKU', 'CheckPointId',
                   'CheckPointName', 'Area', 'TransID', 'TransactionDate', 'SnapshotTime',
                   'DwellTime_calendar', 'DwellTime_working', 'OrderType', 'FactoryStatus',
                   'ProductType', 'NotShippedTransaction_flag', 'ETL_time', 'isFrom_WIP']
    distinctSN_count = rawData_df['SerialNumber'].nunique()

    # Eliminate serial numbers from WIP table that do not match the newest raw extract
    mask = latest_wip_status_df['SerialNumber'].isin(rawData_df['SerialNumber'])
    latest_wip_status_df = latest_wip_status_df[mask]
    latest_wip_status_df = latest_wip_status_df.drop(columns=['LatestUpdateDate'])
    latest_wip_status_df = latest_wip_status_df.rename(columns={'CheckpointID': 'CheckPointId',
                                                                'CheckpointName': 'CheckPointName',
                                                                'ProcessArea': 'Area',
                                                                'TransactionID': 'TransID',
                                                                'WIP_SnapshotDate': 'SnapshotTime',
                                                                'ExtractionDate': 'ETL_time'})
    latest_wip_status_df['isFrom_WIP'] = 'isFrom_WIP'

    # Reindex the raw data and concatenate with existing data from WIP table
    rawData_df = rawData_df.reindex(columns=wip_columns)
    rawData_df = pd.concat([rawData_df, latest_wip_status_df], ignore_index=True)
    ph_instances_grouped = rawData_df.groupby('SerialNumber')

    # Flags for process progress
    nickel = dime = dime_2 = quarter = dime_3 = dime_4 = half = dime_6 = quarter_3 = dime_8 = ninety = ninety_5 = True

    # Cleaning
    if isServerLevel:  # Cleaning for Server Level WIP
        for serialNumber, ph_instance_df in tqdm(ph_instances_grouped, desc="SR WIP cleaning operation progress"):
            cleaned_wip = ServerHistory(ph_instance_df.reset_index(drop=True))
            wip_list.extend(cleaned_wip.determine_processAndArea())
            if len(wip_list) > 1_000_000:
                master_list.append(wip_list.copy())
                wip_list.clear()
    else:  # Cleaning for Rack Level WIP
        print("RE WIP cleaning operation is running on the background. Progress will show intermittently")
        for serialNumber, ph_instance_df in ph_instances_grouped:
            cleaned_wip = RackHistory(ph_instance_df.reset_index(drop=True))
            wip_list.extend(cleaned_wip.determine_processAndArea())
            if len(wip_list) > 1_000_000:
                master_list.append(wip_list.copy())
                wip_list.clear()

            # Provide loop progress feedback for 5%, 10%, 20%, 25%, 30%, 40%, 50%, 60%, 75%, 80%, 90%, and 95%
            counter += 1
            current_progress = counter / distinctSN_count
            if ninety_5 and current_progress >= 0.95:
                print(f"\nRE WIP cleaning operation at 95% ({distinctSN_count} items to clean)")
                ninety_5 = False
            elif ninety and current_progress >= 0.9:
                print(f"\nRE WIP cleaning operation at 90% ({distinctSN_count} items to clean)")
                ninety = False
            elif dime_8 and current_progress >= 0.8:
                print(f"\nRE WIP cleaning operation at 80% ({distinctSN_count} items to clean)")
                dime_8 = False
            elif quarter_3 and current_progress >= 0.75:
                print(f"\nRE WIP cleaning operation at 75% ({distinctSN_count} items to clean)")
                quarter_3 = False
            elif dime_6 and current_progress >= 0.6:
                print(f"\nRE WIP cleaning operation at 60% ({distinctSN_count} items to clean)")
                dime_6 = False
            elif half and current_progress >= 0.5:
                print(f"\nRE WIP cleaning operation at 50% ({distinctSN_count} items to clean)")
                half = False
            elif dime_4 and current_progress >= 0.4:
                print(f"\nRE WIP cleaning operation at 40% ({distinctSN_count} items to clean)")
                dime_4 = False
            elif dime_3 and current_progress >= 0.3:
                print(f"\nRE WIP cleaning operation at 30% ({distinctSN_count} items to clean)")
                dime_3 = False
            elif quarter and current_progress >= 0.25:
                print(f"\nRE WIP cleaning operation at 25% ({distinctSN_count} items to clean)")
                quarter = False
            elif dime_2 and current_progress >= 0.2:
                print(f"\nRE WIP cleaning operation at 20% ({distinctSN_count} items to clean)")
                dime_2 = False
            elif dime and current_progress >= 0.1:
                print(f"\nRE WIP cleaning operation at 10% ({distinctSN_count} items to clean)")
                dime = False
            elif nickel and current_progress >= 0.05:
                print(f"\nRE WIP cleaning operation at 5% ({distinctSN_count} items to clean)")
                nickel = False
        print(f"\nRE WIP data is done cleaning. Duration: {dt.now() - cleaning_start}\n")

    # Convert WIP list to a dataframe
    allocation_start = dt.now()
    if 0 < len(wip_list) <= 1_000_000:  # Still append for wip_list less than 1 million
        master_list.append(wip_list.copy())
        wip_list.clear()
    wip_dfs_list = []

    for wip_list in tqdm(master_list, total=len(master_list), desc=f"Allocating the cleaned "
                                                                   f"{'SR' if isServerLevel else 'RE'} WIP data"):
        wip_df = pd.DataFrame(wip_list, columns=wip_columns)
        # Drop the temporary column 'isFrom_WIP'
        wip_df = wip_df.drop(columns=['isFrom_WIP'])
        # Dwell time calculation
        if wip_df.shape[0] > 0:
            # Convert python Datetime(s) to SQL Datetime
            wip_df[['TransactionDate', 'SnapshotTime']] = wip_df[['TransactionDate', 'SnapshotTime']].applymap(
                datetime_from_py_to_sql)
            wip_df['ETL_time'] = datetime_from_py_to_sql(dt.now())
            wip_dfs_list.append(wip_df.copy())

    if len(wip_dfs_list) < 1:
        final_wip_df = pd.DataFrame([], columns=wip_columns)  # Dummy DF to avoid producing an error
    else:
        print(f"Concatenating the {'SR' if isServerLevel else 'RE'} dataframes in the background...")
        final_wip_df = pd.concat(wip_dfs_list, ignore_index=True)
    print(f"Cleaned {'SR' if isServerLevel else 'RE'} WIP data allocation completed successfully in "
          f"{dt.now() - allocation_start}\n")

    # Store results
    if isServerLevel:
        result_store[0] = final_wip_df
    else:
        result_store[1] = final_wip_df


def assign_shipmentStatus(db_conn):
    """
    Function gets all units that did not have a latest shipment status and updates the current dwell time. If a new
    shipment status exists since the latest INSERT run, then the NotShipped flag is updated accordingly
    :param db_conn: The connection to the database
    :return: a dataframe with the latest shipment information updated
    :rtype: pandas.Dataframe
    """
    criticalShipment_ckps = {300, 301, 302}
    today_upperBoundary = fixed_date(select_wip_maxDate(db_conn, snapshotTime=True))

    wip_df = select_wip_maxStatus(db_conn)
    wip_df = wip_df.drop(columns=['LatestUpdateDate'])
    wip_df_columns = wip_df.columns.tolist()

    wip_grouped = wip_df.groupby('SerialNumber')

    wip_shipped_tuples = []
    wip_stillNotShipped_tuples = []

    # Find the units that do not have Shipping Scan (for rack shipment) nor Carton Scan (for single servers)
    for serialNumber, wipHistory_df in tqdm(wip_grouped, desc="Assigning Shipment Status"):
        wipHistory_df = wipHistory_df.sort_values('WIP_SnapshotDate', ascending=False)
        max_ckp = wipHistory_df['CheckpointID'].iloc[0]
        max_timestamp = wipHistory_df['TransactionDate'].max(skipna=True)
        instance_upper_boundary = wipHistory_df['WIP_SnapshotDate'].max(skipna=True)

        if max_ckp in criticalShipment_ckps:
            wip_shipped_instance = (serialNumber, False)
            wip_shipped_tuples.append(wip_shipped_instance)
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
    wip_shipped_df = pd.DataFrame(wip_shipped_tuples, columns=['SerialNumber', 'ShipmentStatus'])

    # Add the latest update time and make necessary type conversions for SQL
    wip_shipped_df['LatestUpdateTime'] = datetime_from_py_to_sql(dt.now())
    wip_shipped_df = wip_shipped_df.reindex(columns=['ShipmentStatus', 'LatestUpdateTime', 'SerialNumber'])

    # Create the WIP not-shipped dataframe
    wip_stillNotShipped_df = pd.DataFrame(wip_stillNotShipped_tuples, columns=wip_df_columns)
    wip_stillNotShipped_df['ExtractionDate'] = datetime_from_py_to_sql(dt.now())
    wip_stillNotShipped_df['TransactionDate'] = wip_stillNotShipped_df['TransactionDate'].applymap(datetime_from_py_to_sql)
    wip_stillNotShipped_df['WIP_SnapshotDate'] = \
        wip_stillNotShipped_df['WIP_SnapshotDate'].applymap(datetime_from_py_to_sql)

    # Create a dataframe containing only the necessary SNs from wip_stillNotShipped_df that needs update on SQL
    grouped_notShipped = wip_stillNotShipped_df.groupby('SerialNumber')
    unshipped_toUpdate = set()
    for serialNumber, notShipped_df in tqdm(grouped_notShipped, desc="Assessing unshipped Serial Numbers to update"):
        if notShipped_df['NotShippedTransaction_flag'].all():
            continue
        unshipped_toUpdate.add(serialNumber)

    unshipped_toUpdate_df = pd.DataFrame(unshipped_toUpdate, columns=['SerialNumber'])

    return wip_shipped_df, unshipped_toUpdate_df, wip_stillNotShipped_df
