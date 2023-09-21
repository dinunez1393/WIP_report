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


A problem was encountered for Server: rack build, end of line, and rack hi-pot data when updating the WIP table. Since
these checkpoints depend on past and future checkpoints. One example to demonstrate this problem is on rack build. The
data does not update correctly the new extractions get data that is after the MAX WIP transaction date, so past
server assembly finish data does not exist in the new extraction. Whereas for SLT-check-in, which is used to link the
server data to the rack build data, for most servers it does not exist because it is in the future, and once it becomes
available, the server data is already in the WIP table and it would become very hard to update. For the past data there
can be a solution, which is to make a new extraction that looks only for the past checkpoints data, however, for the
future checkpoints, which are crucial because they are used not only to provide an upper bound, but they also have
the rack linking information, there is no solution because the data does not exist yet. In order to conserve the
integrity of the table by keeping server WIP data for rack checkpoints (rack build, End of Line, and rack HiPot) it is
recommended to update the whole table by truncating a re-building, everytime the WIP table needs to be updated.
Moreover, for server data on rack checkpoints, the MAX WIP Snapshot date will never have accurate information, because
as mentioned earlier, it lacks future checkpoint data. For past WIP Snapshot dates, however, it should be accurate
enough.


**Miscellaneous:**
- When using parameters (?) to INSERT data into SQL, the server only accepts a maximum of 2100 parameters.
- When INSERTING rows into SQL by using the VALUES key, the server only accepts a maximum of 1000 rows.
