@echo off
echo Running raw data extraction
cd "C:\Users\diego.nunez\Documents\CodeSource\Python\WIP_Report\src"
python "main.py"
echo Raw data extraction completed.

echo Running data transformation and loading
python "main.py"
echo Script completed.
