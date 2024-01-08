# Script to start other Python scripts

import os
import sys
import json
from pathlib import Path
from datetime import datetime as dt
from utilities import show_message
from alerts import *


program_start = dt.now()
main_path = rf"{Path(__file__).parent}{os.sep}main.py"
programMetaData_path = rf"{Path(__file__).parent}{os.sep}meta_data.json"

print("__________________________________________________________________________________________________\n\n")
print("WIP 1st part: RAW DATA EXTRACTION\n_______________________________________\n\n")
os.system(rf"python {main_path}")
print("WIP 1st part: RAW DATA EXTRACTION COMPLETED\n_______________________________________\n\n")

print("__________________________________________________________________________________________________\n\n")
print("WIP 2nd part: DATA TRANSFORMATION AND LOADING\n_______________________________________\n\n")
os.system(rf"python {main_path}")
print("WIP 2nd part: DATA TRANSFORMATION AND LOADING COMPLETED\n_______________________________________\n\n")
print(f"TOTAL PROGRAM DURATION: {dt.now() - program_start}\n\n")

# Check program satus (success/fail)
with open(programMetaData_path, 'r') as meta_data_file:
    meta_data = json.load(meta_data_file)
    program_success = meta_data['program_success']

if program_success:
    show_message(AlertType.SUCCESS)
else:
    show_message(AlertType.FAILED)

# Reset meta-data flags
meta_data['program_semaphore'] = meta_data['program_success'] = 0
with open(programMetaData_path, 'w') as meta_data_file:
    json.dump(meta_data, meta_data_file)

sys.exit()
