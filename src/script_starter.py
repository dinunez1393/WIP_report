# Script to start other Python scripts

import os
import sys
import extern_vars
from pathlib import Path
from datetime import datetime as dt
from utilities import show_message
from alerts import *

program_start = dt.now()
main_path = rf"{Path(__file__).parent}{os.sep}main.py"

print("__________________________________________________________________________________________________\n\n")
print("WIP 1st part: RAW DATA EXTRACTION\n_______________________________________\n\n")
os.system(rf"python {main_path}")
print("WIP 1st part: RAW DATA EXTRACTION COMPLETED\n_______________________________________\n\n")

print("__________________________________________________________________________________________________\n\n")
print("WIP 2nd part: DATA TRANSFORMATION AND LOADING\n_______________________________________\n\n")
os.system(rf"python {main_path}")
print("WIP 2nd part: DATA TRANSFORMATION AND LOADING COMPLETED\n_______________________________________\n\n")
print(f"TOTAL PROGRAM DURATION: {dt.now() - program_start}\n\n")

if extern_vars.success_flag:
    show_message(AlertType.SUCCESS)
else:
    show_message(AlertType.FAILED)
sys.exit()
