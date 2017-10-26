from heapq import heappop, heappush, heappushpop
from operator import itemgetter
import csv


class DataBase:
    def __init__(self):
        self.id_zip_map  = dict()
        self.id_date_map = dict()

    def insert_record(self, key, amount):

        # choose map
        my_map = self.id_zip_map if len(key[1]) == 5 else self.id_date_map

        if key not in my_map:
            my_map[key] = [[], [], 0]
        my_map[key][2] += amount
        min_heap, max_heap = my_map[key][0], my_map[key][1]
        heappush(min_heap, -heappushpop(max_heap, amount))
        if len(max_heap) < len(min_heap):
            heappush(max_heap, -heappop(min_heap))

    def find_sum(self, key):
        my_map = self.id_zip_map if len(key[1]) == 5 else self.id_date_map
        return int(round(my_map[key][2]))

    def find_record_number(self, key):
        my_map = self.id_zip_map if len(key[1]) == 5 else self.id_date_map

        min_heap, max_heap = my_map[key][0], my_map[key][1]
        return len(min_heap) + len(max_heap)

    def find_median(self, key):
        my_map = self.id_zip_map if len(key[1]) == 5 else self.id_date_map

        min_heap, max_heap = my_map[key][0], my_map[key][1]
        if len(max_heap) > len(min_heap):
            return int(round(max_heap[0]))
        return int(round((max_heap[0] - min_heap[0]) / 2.0))


class InputOutput:

    def __init__(self, input_filename, output_filename_id_zip, output_filename_id_date, delimiter):
        self.input_filename = input_filename
        self.output_filename_id_zip = output_filename_id_zip
        self.output_filename_id_date = output_filename_id_date
        self.database = DataBase()
        self.delimiter = delimiter

    def parse_record_line(self, line, delimiter):
        ''' parse the single input

        :return: cmtd_id, zipcode, transaction date, transaction amount, other id respectively
        '''
        record = line.split(delimiter)
        return record[0], record[10], record[13], record[14], record[15]

    def is_valid_id(self, token):
        return len(token) > 0

    def is_valid_transaction_amount(self, token):
        try:
            float(token)
        except ValueError:
            return False
        return True

    def parse_zip_code(self, token):
        ''' parse a zipcode is valid or not

        :param token: str
        :return: None if invalid else the first five digits
        '''
        if len(token) < 5 or not token[0:5].isdigit():
            return None
        return token[0:5]

    def is_valid_date(self, token):
        ''' parse a string is a valid or not

        :param token: str
        :return: True if valid else False
        '''

        if len(token) != 8 or not token.isdigit():
            return False
        mm, dd, yy = int(token[0:2]), int(token[2:4]), int(token[4:8])
        if mm == 0 or mm > 12 or dd == 0 or dd > 31 or yy < 1900:
            return False
        if mm in [4, 6, 9, 11] and dd == 31:
            return False
        if (mm == 2 and dd >= 30) or ((yy % 4 != 0) and mm == 2 and dd >= 29):
            return False
        return True

    def transform_date_mmddyyyy_to_yyyymmdd(self, date):
        return date[4:8] + date[0:4]

    def transform_date_yyyymmdd_to_mmddyyyy(self, date):
        return date[4:8] + date[0:4]

    def output_id_zipcode_records(self, filename, delimiter):
        my_map = self.database.id_date_map

    def output_id_date_records(self, filename, delimiter):
        my_map = self.database.id_date_map
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

    def read_and_process_file(self):
        # open file
        input_file_handle = open(self.input_filename, 'r')
        output_file_handle = open(self.output_filename_id_zip, 'w')
        writer = csv.writer(output_file_handle, delimiter=self.delimiter)
        for line in input_file_handle:
            recipient_id, zip_code_token, transaction_dt, transaction_amt, other_id = self.parse_record_line(line, self.delimiter)

            # pass if either one the three conditions is True:
            # 1. recipient_id is invalid
            # 2. transaction_amt is invalid
            # 3. other_id is valid
            if not self.is_valid_id(recipient_id) or self.is_valid_id(other_id):
                continue
            if not self.is_valid_transaction_amount(transaction_amt):
                continue

            # check valid zip or not

            zip_code = self.parse_zip_code(zip_code_token)
            amount = float(transaction_amt)
            if zip_code:
                key = (recipient_id, zip_code)
                self.database.insert_record(key, amount)
                my_data = [key[0], key[1], self.database.find_median(key),
                           self.database.find_record_number(key), self.database.find_sum(key)]
                writer.writerow(my_data)

            # check valid date or not
            if self.is_valid_date(transaction_dt):
                key = (recipient_id, self.transform_date_mmddyyyy_to_yyyymmdd(transaction_dt))
                self.database.insert_record(key, amount)

        input_file_handle.close()
        output_file_handle.close()

        # output_file_handle.close()
        # processing id date output file
        self.output_id_date_records(self.output_filename_id_date, '|')


if __name__ == '__main__':
    input_filename = '../input/itcont.txt'
    output_filename_id_zip = '../output/median_id_zip.txt'
    output_filename_id_date = '../output/median_id_date.txt'
    io = InputOutput(input_filename, output_filename_id_zip, output_filename_id_date, '|')
    io.read_and_process_file()

