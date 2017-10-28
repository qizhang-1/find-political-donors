from heapq import heappop, heappush, heappushpop
from operator import itemgetter
import csv


class DataBase:
    def __init__(self):
        ''' store the data in the form of key-value pair in dictionaries
        the value of of a key has 3 entries (minHeap, maxHeap and the sum)
        median is calculated based upon the heaps
        '''

        self.id_zip_map = dict()
        self.id_date_map = dict()

    def select_dictionary(self, key):
        ''' select the dictionary based on the type of the key

        :param key: tuple
        :return: the dictionary based on the length of the second entry
        '''
        return self.id_zip_map if len(key[1]) == 5 else self.id_date_map

    def insert_record(self, key, amount):
        ''' insert the key value pair into the dictionary

        :param key: tuple, with 2 entries, (recipient ID <- str and date/zip <- str)
        :param amount: float, amount the money
        :return:
        '''

        my_map = self.select_dictionary(key)
        # insert new key if the key is NOT in the map
        if key not in my_map:
            my_map[key] = [[], [], 0]

        # update the total (sum) first
        my_map[key][2] += amount

        # adjust the min/max heaps
        min_heap, max_heap = my_map[key][0], my_map[key][1]
        heappush(min_heap, -heappushpop(max_heap, amount))
        if len(max_heap) < len(min_heap):
            heappush(max_heap, -heappop(min_heap))

    def find_sum(self, key):
        '''
        :param key: tuple
        :return: the total amount of the money received by the recipient
        '''
        my_map = self.select_dictionary(key)
        return int(round(my_map[key][2]))

    def find_record_number(self, key):
        '''
        :param key: tuple
        :return: the total number of the donations received by the recipient
        '''
        my_map = self.select_dictionary(key)
        return len(my_map[key][0]) + len(my_map[key][1])

    def find_median(self, key):
        ''' the median of the donations

        :param key: tuple
        :return: the median of the donations
        '''
        def myround(x):
            if abs(x) < 0.001:
                return 0
            return int(x + 0.5001) if x > 0 else int(x - 0.5001)
        my_map = self.select_dictionary(key)

        min_heap, max_heap = my_map[key][0], my_map[key][1]
        if len(max_heap) > len(min_heap):
            return myround(max_heap[0])
        return myround((max_heap[0] - min_heap[0]) / 2.0)


class InputOutput:

    def __init__(self, input_filename, output_filename_id_zip, output_filename_id_date, delimiter):

        self.input_filename = input_filename
        self.output_filename_id_zip = output_filename_id_zip
        self.output_filename_id_date = output_filename_id_date
        self.database = DataBase()
        self.delimiter = delimiter

    def parse_single_input_line(self, line, delimiter):
        ''' parse the single input

        :return: cmtd_id, zipcode, transaction date, transaction amount, other id respectively
        '''

        def get_transaction_amount(val):
            try:
                float(val)
            except ValueError:
                return None
            return float(val)

        def get_date(token):

            ''' parse a string is a valid or not

            :param token: str
            :return: original string if valid else None
            '''

            if len(token) != 8 or not token.isdigit():
                return None
            mm, dd, yy = int(token[0:2]), int(token[2:4]), int(token[4:8])
            if mm == 0 or mm > 12 or dd == 0 or dd > 31 or yy < 1900:
                return None
            if mm in [4, 6, 9, 11] and dd == 31:
                return None
            if (mm == 2 and dd >= 30) or ((yy % 4 != 0) and mm == 2 and dd >= 29):
                return None
            return token
        
        tokens = line.split(delimiter)
        token = tokens[0].strip()
        cmtd_id = token if len(token) != 0 else None
        token = tokens[10].strip()
        zipcode = token[0:5] if len(tokens[10]) >= 5 and token[0:5].isdigit() else None
        token = tokens[13].strip()
        date = get_date(token)
        token = tokens[14].strip()
        amount = get_transaction_amount(token)
        token = tokens[15].strip()
        other_id = token if len(token) != 0 else None
        return cmtd_id, zipcode, date, amount, other_id

    @staticmethod
    def transform_date_mmddyyyy_to_yyyymmdd(date):
        return date[4:8] + date[0:4]

    @staticmethod
    def transform_date_yyyymmdd_to_mmddyyyy(date):
        return date[4:8] + date[0:4]

    def output_complete_records(self, my_map, filename, delimiter):
        ''' output the complete database stored in the dictionary

        :param my_map: dictionary of the datebase
        :param filename: output file
        :param delimiter: splitter (i.e. ',' or '|' or ' ' etc.)
        :return: None
        '''

        my_data = []
        for key in sorted(my_map.keys(), key=itemgetter(0, 1)):
            record = [key[0], self.transform_date_yyyymmdd_to_mmddyyyy(key[1]), self.database.find_median(key),
                      self.database.find_record_number(key), self.database.find_sum(key)]
            my_data.append(record)

        file_handle = open(filename, 'w')
        with file_handle:
            writer = csv.writer(file_handle, delimiter=delimiter)
            writer.writerows(my_data)
        file_handle.close()

    def read_process_write(self):
        # open input file
        input_file_handle = open(self.input_filename, 'r')
        output_file_handle = open(self.output_filename_id_zip, 'w')
        writer = csv.writer(output_file_handle, delimiter=self.delimiter)
        k = 0
        for line in input_file_handle:
            cmtd_id, zipcode, date, amount, other_id = self.parse_single_input_line(line, self.delimiter)
            # pass if either one the three conditions is True:
            # 1. cmtd_id is invalid
            # 2. transaction_amt is invalid
            # 3. other_id is valid

            if cmtd_id is None or amount is None or other_id is not None:
                continue

            # check valid zipcode or not
            if zipcode:
                key = (cmtd_id, zipcode)
                self.database.insert_record(key, amount)
                my_data = [key[0], key[1], self.database.find_median(key),
                           self.database.find_record_number(key), self.database.find_sum(key)]
                writer.writerow(my_data)

            # check valid date or not
            if date:
                key = (cmtd_id, self.transform_date_mmddyyyy_to_yyyymmdd(date))
                self.database.insert_record(key, amount)
        input_file_handle.close()
        output_file_handle.close()

        # write id_date file
        self.output_complete_records(self.database.id_date_map, self.output_filename_id_date, self.delimiter)

# for test purpose only
if __name__ == '__main__':
    input_filename = '../input/itcont.txt'
    output_filename_id_zip = '../output/medianvals_by_zip.txt'
    output_filename_id_date = '../output/medianvals_by_date.txt'
    io = InputOutput(input_filename, output_filename_id_zip, output_filename_id_date, '|')
    io.read_process_write()

