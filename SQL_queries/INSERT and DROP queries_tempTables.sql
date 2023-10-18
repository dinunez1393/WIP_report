/****** Script for SelectTopNRows command from SSMS  ******/
SELECT TOP (100000) [Site]
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
      ,[PackedPreviously_flag]
      ,[ExtractionDate]
  FROM [SBILearning].[dbo].[DNun_tbl_Production_WIP_history]
  WHERE SerialNumber = '210185500001'
  ORDER BY WIP_SnapshotDate DESC;

  SELECT COUNT(*)
  FROM [SBILearning].[dbo].[DNun_tbl_Production_WIP_history];

  INSERT INTO [SBILearning].[dbo].[DNun_tbl_Production_WIP_history]
  SELECT * FROM [SBILearning].[dbo].[wip_sr_records_1];

  
  INSERT INTO [SBILearning].[dbo].[DNun_tbl_Production_WIP_history]
  SELECT * FROM [SBILearning].[dbo].[wip_sr_records_2];

  
  INSERT INTO [SBILearning].[dbo].[DNun_tbl_Production_WIP_history]
  SELECT * FROM [SBILearning].[dbo].[wip_sr_records_3];

  
  INSERT INTO [SBILearning].[dbo].[DNun_tbl_Production_WIP_history]
  SELECT * FROM [SBILearning].[dbo].[wip_sr_records_4];

  
  INSERT INTO [SBILearning].[dbo].[DNun_tbl_Production_WIP_history]
  SELECT * FROM [SBILearning].[dbo].[wip_re_records_5];

  DROP TABLE [SBILearning].[dbo].[wip_sr_records_1];
  DROP TABLE [SBILearning].[dbo].[wip_sr_records_2];
  DROP TABLE [SBILearning].[dbo].[wip_sr_records_3];
  DROP TABLE [SBILearning].[dbo].[wip_sr_records_4];
  DROP TABLE [SBILearning].[dbo].[wip_re_records_5];





  

  