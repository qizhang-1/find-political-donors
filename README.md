# Table of Contents
1. [Introduction](README.md#introduction)
2. [Instructions](README.md#instructions)

# Introduction
In this code challenge, two different methods are implemented in this challenge.
The first method starts from reading the csv file line by line, for those qualified records are stored in two different (key, value) pair dictionaries.   The key is a tuple with two elements (recipient ID, date) or (recipient ID, zip code) while the value has three components.   Two heaps (one minimum heap and one maximum heap) are used to compute the cumulative median and the last component is the sum of the amount received at the time the current record line is processed.
The second methods uses pandas library heavily to preprocess the data.    The part of the code heavily uses the built-in functions in pandas.  For the cumulative median part, a similar data structure (see above) is applied.


# Instructions
The default submission uses the first method which reads and processes the data in a streaming manner.  To run the first code, just type `sh run.sh`;

To run the second code, comment the command `python3 ./src/find_political_donors.py ./input/itcont.txt ./output/medianvals_by_zip.txt ./output/medianvals_by_date.txt` in `run.sh` and
uncomment the command `python3 ./src/find_political_donors2.py ./input/itcont.txt ./output/medianvals_by_zip.txt ./output/medianvals_by_date.txt`

Both codes should yield the same results although the first one is at least 2 times faster.


