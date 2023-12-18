@echo off
echo Running raw data extraction
python "main.py"
echo Raw data extraction completed.

echo Running data transformation and loading
python "main.py"
echo Script completed.
