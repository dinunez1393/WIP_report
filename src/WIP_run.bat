@echo off
echo Running raw data extraction
python "src\main.py"
echo Raw data extraction completed.

echo Running data transformation and loading
python "src\main.py"
echo Script completed.
