@echo off
for /l %%i in (1, 1, 6) do (
    echo Site,Building,SerialNumber,StockCode,SKU,CheckPointId,CheckPointName,Area,TransID,TransactionDate,SnapshotTime,DwellTime_calendar,DwellTime_working,OrderType,FactoryStatus,ProductType,NotShippedTransaction_flag,ETL_time>> wip_sr_records_%%i.csv
)
