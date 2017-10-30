#!/bin/bash
#
#
# Use this shell script to compile (if necessary) your code and then execute it. Below is an example of what might be found in this file if your program was written in Python
#
# process the data w/o using pandas
python3 ./src/find_political_donors.py ./input/itcont.txt ./output/medianvals_by_zip.txt ./output/medianvals_by_date.txt
# process the data using pandas
python3 ./src/find_political_donors2.py ./input/itcont.txt ./output/medianvals_by_zip2.txt ./output/medianvals_by_date2.txt

