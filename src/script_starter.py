# Script to start other Python scripts

import os
from pathlib import Path


main_path = rf"{Path(__file__).parent}{os.sep}main.py"

print("__________________________________________________________________________________________________\n\n")
print("WIP 1st part: RAW DATA EXTRACTION\n_______________________________________\n\n")
os.system(rf"python {main_path}")
print("WIP 1st part: RAW DATA EXTRACTION COMPLETED\n_______________________________________\n\n")

print("__________________________________________________________________________________________________\n\n")
print("WIP 2nd part: DATA TRANSFORMATION AND LOADING\n_______________________________________\n\n")
os.system(rf"python {main_path}")
print("WIP 2nd part: DATA TRANSFORMATION AND LOADING COMPLETED\n_______________________________________\n\n")
