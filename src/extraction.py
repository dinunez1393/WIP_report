# SELECT SQL queries
from utilities import *
from alerts import *
import pandas as pd
from datetime import datetime as dt, timedelta, date
import sys

# To build table from scratch
if dt.now().date() == date(2023, 12, 15):
    DATE_THRESHOLD = dt(2023, 12, 7, 0, 0)
else:  # Normal runs
    DATE_THRESHOLD = dt.now() - timedelta(days=400)

LOGGER = logger_creator('SELECT_Error')


def select_wipTable_count(db_conn):
    """
    Function gets the number of items in the WIP table
    :param db_conn: The connection to the database
    :return: The number of items from the WIP table
    :rtype: int
    """
    query = "SELECT COUNT(*) FROM [SBILearning].[dbo].[DNun_tbl_Production_OngoingWIP_Actual];"

    try:
        with db_conn.cursor() as cursor:
            cursor.execute(query)
            table_count = int(cursor.fetchone()[0])
    except Exception as e:
        print(repr(e))
        LOGGER.error(Messages.SQL_Q_ERROR.value, exc_info=True)
        show_message(AlertType.FAILED)
        sys.exit()
    else:
        return table_count


def select_wip_maxDate(db_conn, snapshotTime=False):
    """
    SELECT function to get the maximum date from the SQL WIP table
    :param db_conn: The connection to access the DB
    :param snapshotTime: Flag to choose MAX snapshot time or not
    :return: A datetime object
    :rtype: datetime.datetime
    """
    query = f"SELECT MAX([{'WIP_SnapshotDate' if snapshotTime else 'TransactionDate'}])" \
            f" FROM [SBILearning].[dbo].[DNun_tbl_Production_OngoingWIP_Actual];"
    print("SELECT process for MAX date from WIP is running in the background...")

    try:
        time_tracker = dt.now()
        with db_conn.cursor() as cursor:
            cursor.execute(query)
            max_date = cursor.fetchone()
            max_date = max_date[0]
    except Exception as e:
        print(repr(e))
        LOGGER.error(Messages.SQL_Q_ERROR.value, exc_info=True)
        show_message(AlertType.FAILED)
        sys.exit()
    else:
        if max_date is None:  # Table is empty
            max_date = DATE_THRESHOLD
        print(f"SELECT process for MAX date from WIP ran successfully. The latest MAX date is: {max_date}\n"
              f"T: {dt.now() - time_tracker}\n")
        return max_date


async def select_customers(async_pool):
    """
    Function gets all the customers with their respective stock codes from SAP and Agile.
    :param async_pool: The asynchronous pool to access the DB
    :return: A dataframe containing customers and stock codes
    :rtype: pandas.DataFrame
    """
    query_sap = """
                SELECT DISTINCT CUSTID AS Customer, MATNR AS StockCode
                FROM [sapdb].[sap].[ZTFTP_ZTPOK_ProductionScheduleHeader];
                """
    query_azu = """
                SELECT DISTINCT MSPARTNUM AS Customer, STOCKCODE AS StockCode
                  FROM [ASBuiltDW].[dbo].[tbl_ref_agile_ITEM]
                  WHERE MSPARTNUM IS NOT NULL AND MSPARTNUM != 'N/A'
                  AND (STOCKCODE LIKE 'SR-%' OR STOCKCODE LIKE 'JB-%' OR STOCKCODE LIKE 'FR-%'
                  OR STOCKCODE LIKE 'RE-%');
                """
    query_amz = """
                SELECT DISTINCT AZPARTNUM AS Customer, STOCKCODE AS StockCode
                  FROM [ASBuiltDW].[dbo].[tbl_ref_agile_ITEM]
                  WHERE AZPARTNUM IS NOT NULL AND AZPARTNUM != 'N/A'
                  AND (STOCKCODE LIKE 'SR-%' OR STOCKCODE LIKE 'JB-%' OR STOCKCODE LIKE 'FR-%'
                  OR STOCKCODE LIKE 'RE-%');
                """

    print("SELECT process for customers information is running on the background...\n")
    try:
        time_tracker = dt.now()
        async with async_pool.acquire() as db_conn:
            async with db_conn.cursor() as cursor:
                await cursor.execute(query_sap)
                rows = await cursor.fetchall()
                cols = [column[0] for column in cursor.description]
                customers_df = pd.DataFrame.from_records(rows, columns=cols)

                await cursor.execute(query_azu)
                rows = await cursor.fetchall()
                cols = [column[0] for column in cursor.description]
                azu_customer_df = pd.DataFrame.from_records(rows, columns=cols)

                await cursor.execute(query_amz)
                rows = await cursor.fetchall()
                cols = [column[0] for column in cursor.description]
                amz_customer_df = pd.DataFrame.from_records(rows, columns=cols)
    except Exception as e:
        print(repr(e))
        LOGGER.error(Messages.SQL_Q_ERROR.value, exc_info=True)
        show_message(AlertType.FAILED)
        sys.exit()
    else:
        azu_customer_df['Customer'] = 'AZU'
        amz_customer_df['Customer'] = 'AMZ'
        customers_df = pd.concat([customers_df, azu_customer_df, amz_customer_df], ignore_index=True)
        customers_df.dropna(inplace=True)
        customers_df.drop_duplicates(subset=['StockCode'], inplace=True)
        print(f"SELECT process for customers ran successfully. T: {dt.now() - time_tracker}\n")
        return customers_df


async def select_ph_rawData(async_pool, date_threshold):
    """
    SELECT function to get all the raw data from product history
    :param async_pool: The asynchronous pool to access the DB
    :param date_threshold: the datetime in SQL format used as the lower limit of data extraction
    :type date_threshold: str
    :return: A tuple containing two dataframes, one for SR and the other for RE,
    with all raw elements from product history
    :rtype: tuple
    """
    query = f"""
        SELECT 'NJ' AS Site
            ,CASE WHEN ph.[Location] LIKE '%350%'
                THEN 'B350'
                WHEN ph.[Location] LIKE '%[(40-50)|(4050)]%'
                THEN 'B4050'
                ELSE 'Unknown'
                END AS Building
          ,ph.[SerialNumber]
          ,ph.[StockCode]
          ,SUBSTRING(ph.[StockCode], 1, 2) AS StockCodePrefix
          ,ph.[StringField1]
          ,ph.[CheckPointId]
          ,ph.[CheckPointName]
          ,ph.[TransactionDate]
          ,ph.[Success]
          ,ph.[Message]
          ,ph.[TransID]
          ,agi_SS.[SKU]
          ,sap_mfgPO.[OrderTypeCode] AS OrderType
      FROM [ASBuiltDW].[dbo].[producthistory] AS ph
      INNER JOIN [ASBuiltDW].[dbo].[tbl_ref_agile_SSCode] AS agi_SS
            ON ph.StockCode = agi_SS.ITEM_NUMBER
      LEFT JOIN [ASBuiltDW].[dbo].[tbl_Manufacturing_ProductionOrdersSAP_Current] AS sap_mfgPO
            ON SUBSTRING(ph.[SerialNumber], 1, 8) = sap_mfgPO.[JobOrder]
      WHERE ph.[Site] = 'NJ' AND ph.TransactionDate > '{date_threshold}'
      AND ph.TransactionDate <= CONCAT(CAST(GETDATE() AS DATE), ' 15:00')
      AND LEN(ph.SerialNumber) = 12
      AND (ph.[StockCode] LIKE 'RE-%' OR ph.[StockCode] LIKE 'SR-%' OR ph.[StockCode] LIKE 'JB-%'
            OR ph.[StockCode] LIKE 'FR-%')
      AND ph.[CheckPointId] IN (100, 101, 200, 235, 236,
                           254, 255, 208, 209,
                           252, 253, 201, 150, 102,
                           170, 216, 217, 218, 219,
                           260, 202, 243, 224,
                           247, 228, 229,
                           270, 237, 230, 231,
                           300, 301,
                           151, 234, 302,
                           700, 801);
    """
    print("SELECT process for Server raw data from [ASBuiltDW].[dbo].[producthistory] running in the background...\n")
    try:
        time_tracker = dt.now()
        async with async_pool.acquire() as db_conn:
            async with db_conn.cursor() as cursor:
                await cursor.execute(query)
                rows = await cursor.fetchall()
                cols = [column[0] for column in cursor.description]
                ph_rawData_df = pd.DataFrame.from_records(rows, columns=cols)
    except Exception as e:
        print(repr(e))
        LOGGER.error(Messages.SQL_Q_ERROR.value, exc_info=True)
        show_message(AlertType.FAILED)
        sys.exit()
    else:
        # Ensure correct Serial Numbers by cleaning leading and trailing spaces
        ph_rawData_df['SerialNumber'] = ph_rawData_df['SerialNumber'].str.strip()
        ph_rawData_df['StringField1'] = ph_rawData_df['StringField1'].str.strip()
        # Re-assure TransactionDate is of type datetime
        ph_rawData_df['TransactionDate'] = pd.to_datetime(ph_rawData_df['TransactionDate'])

        # Separate instances of Hipot Start for both RE and SR
        re_hipotStart_df = (ph_rawData_df[(ph_rawData_df['CheckPointId'] == 247) & (ph_rawData_df['Success'] == 0) &
                                          (ph_rawData_df['Message'] == 'Test Start')])
        sr_hipotStart_df = (ph_rawData_df[(ph_rawData_df['CheckPointId'] == 151) & (ph_rawData_df['Success'] == 0) &
                                          (ph_rawData_df['Message'] == 'Test Start')])
        # For hipot starting points re-assign new checkpoint IDs by adding a 0
        re_hipotStart_df['CheckPointId'] = 2470
        sr_hipotStart_df['CheckPointId'] = 1510
        # Purge all other records with Success = 0
        mask = ph_rawData_df['Success'] != 0
        ph_rawData_df = ph_rawData_df[mask]
        # Combine Hipot Start with rest of data
        ph_rawData_df = pd.concat([ph_rawData_df, re_hipotStart_df, sr_hipotStart_df], ignore_index=True)

        # Separate SR and RE data
        re_mask = ph_rawData_df['StockCode'].str.contains(r"^RE-\d{3,5}-?\d{0,3}")
        sr_mask = ~ph_rawData_df['StockCode'].str.contains(r"^RE-\d{3,5}-?\d{0,3}")
        re_rawData_df = ph_rawData_df[re_mask]
        sr_rawData_df = ph_rawData_df[sr_mask]
        print(f"SELECT process for main raw data ran successfully\nT: {dt.now() - time_tracker}\n")
        return re_rawData_df, sr_rawData_df


async def select_sap_historicalStatus(async_pool, date_threshold):
    """
    SELECT function to get the historical status data from SAP
    :param async_pool: The asynchronous pool to access the DB
    :param date_threshold: the datetime in SQL format used as the lower limit of data extraction
    :type date_threshold: str
    :return: Two dataframes, one for SR and the other for RE,
    containing historical SAP status data: SN, STATUS, Extraction Timestamp
    :rtype: tuple
    """
    query = f"""
        WITH phSNs_CTE AS (
            SELECT DISTINCT [SerialNumber]
            FROM [ASBuiltDW].[dbo].[producthistory]
            WHERE [Site] = 'NJ' AND [TransactionDate] > '{date_threshold}'
              AND [TransactionDate] <= CONCAT(CAST(GETDATE() AS DATE), ' 15:00')
              AND LEN([SerialNumber]) = 12
              AND ([StockCode] LIKE 'RE-%' OR [StockCode] LIKE 'SR-%' OR [StockCode] LIKE 'JB-%'
                    OR [StockCode] LIKE 'FR-%')
              AND [CheckPointId] IN (100, 101, 200, 235, 236,
                                   254, 255, 208, 209,
                                   252, 253, 201, 150, 102,
                                   170, 216, 217, 218, 219,
                                   260, 202, 243, 224,
                                   247, 228, 229,
                                   270, 237, 230, 231,
                                   300, 301,
                                   151, 234, 302)
        )
        SELECT [SERIAL_NO] AS SerialNumber
        ,[MATNR] AS StockCode
        ,[STATUS]
        ,[EXTRACTED_DATE_TIME]
        FROM [sapdb].[sap].[ZTPTP_SNSTATUS_SerialStatus_History]
        WHERE [EXTRACTED_DATE_TIME] > '{date_threshold}'
        AND [BUKRS] = 'US01' AND [SERIAL_NO] IN (SELECT * FROM phSNs_CTE);
    """
    print("SELECT process for SAP Historical Status data is running on the background...\n")
    try:
        time_tracker = dt.now()
        async with async_pool.acquire() as db_conn:
            async with db_conn.cursor() as cursor:
                await cursor.execute(query)
                rows = await cursor.fetchall()
                print(f"SAP Historical Status query execution is done. T: {dt.now() - time_tracker}")
                time_tracker = dt.now()
                cols = [column[0] for column in cursor.description]
                sap_historySatuts_df = pd.DataFrame.from_records(rows, columns=cols)
                print(f"SAP Historical Status dataframe is built. T: {dt.now() - time_tracker}")
                time_tracker = dt.now()
    except Exception as e:
        print(repr(e))
        LOGGER.error(Messages.SQL_Q_ERROR.value, exc_info=True)
        show_message(AlertType.FAILED)
        sys.exit()
    else:
        # Ensure correct Serial Numbers by cleaning leading and trailing spaces
        sap_historySatuts_df['SerialNumber'] = sap_historySatuts_df['SerialNumber'].str.strip()
        # Ensure Extraction date is of type datetime
        sap_historySatuts_df['EXTRACTED_DATE_TIME'] = pd.to_datetime(sap_historySatuts_df['EXTRACTED_DATE_TIME'])
        print(f"SAP Historical Status dataframe data assurance is done. T: {dt.now() - time_tracker}")
        time_tracker = dt.now()
        # Separate SR and RE data
        re_mask = sap_historySatuts_df['StockCode'].str.contains(r"^RE-\d{3,5}-?\d{0,3}")
        sr_mask = ~sap_historySatuts_df['StockCode'].str.contains(r"^RE-\d{3,5}-?\d{0,3}")
        re_sap_statusH_df = sap_historySatuts_df[re_mask]
        sr_sap_statusH_df = sap_historySatuts_df[sr_mask]
        # Drop StockCode column
        re_sap_statusH_df.drop(columns=['StockCode'])
        sr_sap_statusH_df.drop(columns=['StockCode'])
        print(f"SAP Historical Status dataframe is split into two dataframes SR and RE. T: {dt.now() - time_tracker}")
        print("SELECT process for SAP Historical Status data ran successfully\n")
        return sr_sap_statusH_df, re_sap_statusH_df


def select_wip_maxStatus(db_conn, packed=False):
    """
    SELECT function to get the latest WIP status for each serial number that hasn't shipped yet or that it has already
    been packed
    :param db_conn: The connection to the database
    :param packed: Flag to get packed SNs
    :return: A dataframe containing the WIP data of distinct units that have not shipped
    :rtype: pandas.DataFrame
    """
    if packed:
        query = """
            WITH maxTransT_CTE AS (
                SELECT [SerialNumber], MAX([TransactionDate]) AS MaxTransactionDate
                FROM [SBILearning].[dbo].[DNun_tbl_Production_OngoingWIP_Actual]
                WHERE [PackedIsLast_flag] = 1
                GROUP BY [SerialNumber]
            )
            SELECT wip.*
            FROM [SBILearning].[dbo].[DNun_tbl_Production_OngoingWIP_Actual] AS wip
            INNER JOIN maxTransT_CTE AS wipMax
            ON wip.SerialNumber = wipMax.SerialNumber AND wip.TransactionDate = wipMax.MaxTransactionDate
            WHERE wip.[ProductType] = '{}';
        """
    else:
        query = """
            WITH maxTransT_CTE AS (
                SELECT [SerialNumber], MAX([TransactionDate]) AS MaxTransactionDate
                FROM [SBILearning].[dbo].[DNun_tbl_Production_OngoingWIP_Actual]
                WHERE [PackedIsLast_flag] = 0
                GROUP BY [SerialNumber]
            )
            SELECT wip.*
            FROM [SBILearning].[dbo].[DNun_tbl_Production_OngoingWIP_Actual] AS wip
            INNER JOIN maxTransT_CTE AS wipMax
            ON wip.SerialNumber = wipMax.SerialNumber AND wip.TransactionDate = wipMax.MaxTransactionDate
            WHERE wip.[ProductType] = '{}';
        """
    try:
        time_tracker = dt.now()
        print(f"SELECT process for WIP max status of units that {'have been packed' if packed else 'have not shipped'} "
              f"is running in the background...\n")
        wip_lastStatus_server_df = pd.read_sql_query(query.format('Server'), db_conn,
                                                     parse_dates=['TransactionDate', 'WIP_SnapshotDate',
                                                                  'ExtractionDate'])
        wip_lastStatus_rack_df = pd.read_sql_query(query.format('Rack'), db_conn,
                                                   parse_dates=['TransactionDate', 'WIP_SnapshotDate',
                                                                'ExtractionDate'])
    except Exception as e:
        print(repr(e))
        LOGGER.error(Messages.SQL_Q_ERROR.value, exc_info=True)
        show_message(AlertType.FAILED)
        sys.exit()
    else:
        # Use only the latest transaction timestamp and latest WIP snapshot time
        wip_lastStatus_server_df = wip_lastStatus_server_df.loc[wip_lastStatus_server_df.groupby('SerialNumber')
                                                                ['WIP_SnapshotDate'].idxmax()].reset_index(drop=True)
        wip_lastStatus_rack_df = wip_lastStatus_rack_df.loc[wip_lastStatus_rack_df.groupby('SerialNumber')
                                                            ['WIP_SnapshotDate'].idxmax()].reset_index(drop=True)
        print(f"SELECT process for WIP data of distinct units that have not shipped ran successfully\n"
              f"T: {dt.now() - time_tracker}\n")
        return wip_lastStatus_server_df, wip_lastStatus_rack_df
