SELECT [CurrentDwellTime_calendar]
      ,[CurrentDwellTime_working]
      ,[PackedIsLast_flag]
      ,[SerialNumber]
      ,[LatestUpdateTime]
  FROM [SBILearning].[dbo].[wip_updatedRecords]
 -- WHERE PackedIsLast_flag = 'False'

 UPDATE o
 SET o.[CurrentDwellTime_calendar] = CASE WHEN ISNUMERIC(u.[CurrentDwellTime_calendar]) = 1
                                    THEN CAST(u.[CurrentDwellTime_calendar] AS decimal(9,4))
                                    ELSE NULL
                                    END
    ,o.[CurrentDwellTime_working] = CASE WHEN ISNUMERIC(u.[CurrentDwellTime_working]) = 1
                                    THEN CAST(u.[CurrentDwellTime_working] AS decimal(9,4))
                                    ELSE NULL
                                    END
    ,o.[PackedIsLast_flag] = CAST(u.[PackedIsLast_flag] AS bit)
    ,o.[LatestUpdateDate] = CAST(u.[LatestUpdateTime] AS datetime)
FROM [SBILearning].[dbo].[DNun_tbl_Production_WIP_history] o
JOIN [SBILearning].[dbo].[wip_updatedRecords] u
ON o.SerialNumber = u.SerialNumber;


--Update SHIPMENT Flag - SHIPPED
  UPDATE o
  SET o.[PackedIsLast_flag] = 1
  ,o.[LatestUpdateDate] = u.[LatestUpdateTime]
  FROM [SBILearning].[dbo].[DNun_tbl_Production_WIP_history] o
  JOIN [SBILearning].[dbo].[wip_updatedShipped] u
  ON o.[SerialNumber] = u.[SerialNumber]


  --Update SHIPMENT Flag - NOT SHIPPED
  UPDATE o
  SET o.[PackedIsLast_flag] = 0
  ,o.[LatestUpdateDate] = GETDATE()
  FROM [SBILearning].[dbo].[DNun_tbl_Production_WIP_history] o
  JOIN [SBILearning].[dbo].[wip_updatedNotShipped] u
  ON o.[SerialNumber] = u.[SerialNumber]