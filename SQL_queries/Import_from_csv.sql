/****** Script for SelectTopNRows command from SSMS  ******/
SELECT [Site]
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
      ,[ExtractionDate]
      ,[LatestUpdateDate]
 FROM [SBILearning].[dbo].[DNun_tbl_Production_WIP_history]


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
      ,[ExtractionDate]
  )
  SELECT [Site]
      ,[Building]
      ,[SerialNumber]
      ,[StockCode]
      ,[SKU]
      ,CAST([CheckPointId] AS INT)
      ,[CheckPointName]
      ,[Area]
      ,CAST([TransID] AS BIGINT)
      ,CAST([TransactionDate] AS datetime)
      ,CAST([SnapshotTime] AS datetime)
      ,CAST([DwellTime_calendar] AS decimal(9,4))
      ,CAST([DwellTime_working] AS decimal(9,4))
      ,[OrderType]
      ,[FactoryStatus]
	  ,[ProductType]
	  ,CAST([PackedIsLast_flag] AS bit)
      ,CAST([ETL_time] AS datetime)
  FROM [SBILearning].[dbo].[wip_sr_records];
