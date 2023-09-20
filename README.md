# WIP_report usage manual

When using the script to build the SQL table from scratch, make sure to do the following:

1. In main.py do a 'Replace All' (ctrl + R) for all parameters 'to_csv=False'. Change them to 'to_csv=True'
2. In main.py comment out the UPDATE part of the script, which is labeled as 'Update the shipment status from WIP table'
   From this line until the end of the running script should be commented out. Otherwise, this part will overwrite the
   CSV files created previously, which have all the WIP data.
3. In model.py make sure to uncomment the 'minThreshold' variable and the code block for setting the min_timestamp for
   very old instances. The 'minThreshold' variable and the code block are labeled as
   'Use only for initial population of the SQL table'. Do this procedure for both classes: ServerHistory and
   RackHistory.
4. Once the script finishes the 'do it from scratch' operation, make sure to revert all the aforementioned changes for
   regular daily runs of the script.