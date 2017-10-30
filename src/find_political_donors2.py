import sys
import pandas as pd
import numpy as np
from heapq import heappop, heappush, heappushpop
import time

start = time.time()
def myround(x):
    if x == 0:
        return 0
    x = x + 0.501 if x > 0 else x - 0.501
    return int(x)

input_filename = sys.argv[1]
output_filename_id_zip = sys.argv[2]
output_filename_id_date = sys.argv[3]

input_file = '../input/itcont.txt'
input_file = input_filename

df = pd.read_csv(input_file, '|', header=None, dtype='str')
df = df.drop([1,2,3,4,5,6,7,8,9,11,12,16,17,18,19,20],axis=1)
df.columns = ['id', 'zipcode', 'date', 'amount', 'uid']
df = df.loc[df['amount'].notnull()]
df['amount'] = df['amount'].astype(float)
df = df[np.isfinite(df['amount'])]

df = df.loc[df['id'].notnull()]
df = df.loc[df['uid'].isnull()]
df = df.drop(['uid'], axis=1)

# process the date file
# partition
df_date = df.loc[df['date'].notnull()]
df_date.drop(['zipcode'], axis=1, inplace=True)
df_date = df_date.groupby(['id', 'date'])['amount'].agg(['median','count','sum']).reset_index()
df_date['helper'] = df_date['date'].str[4:8] + df_date['date'].str[0:4]
df_date = df_date.sort_values(['id', 'helper'], ascending=[True, True])
df_date.drop(['helper'], axis=1, inplace=True)
# to prevent 0.49999999999 and -0.4999999999 cases
df_date['sum'] = df_date['sum'].apply(myround)
df_date['median'] = df_date['median'].apply(myround)
# save solution file
output_file = '../output/medianvals_by_date2.txt'
output_file = output_filename_id_date

df_date.to_csv(output_file, sep='|',header=None, index=False)

# process the date file
df_zipcode = df.drop(['date'], axis=1)
df_zipcode = df_zipcode.loc[df['zipcode'].notnull()]
df_zipcode = df_zipcode.loc[df_zipcode['zipcode'].str.len() >= 5]
df_zipcode['zipcode'] = df_zipcode['zipcode'].str[0:5]
df_zipcode = df_zipcode.reset_index()

id_zip_map = dict()
data_list = []
for i in range(0, len(df_zipcode.index)):
    key = (df_zipcode['id'][i], df_zipcode['zipcode'][i])
    amount = df_zipcode['amount'][i]
    if key not in id_zip_map:
        id_zip_map[key] = [[], [], 0]
    id_zip_map[key][2] += amount
    min_heap, max_heap = id_zip_map[key][0], id_zip_map[key][1]
    heappush(min_heap, -heappushpop(max_heap, amount))
    if len(max_heap) < len(min_heap):
        heappush(max_heap, -heappop(min_heap))
    sum_ = myround(id_zip_map[key][2])
    cnt_ = len(id_zip_map[key][0]) + len(id_zip_map[key][1])
    median_ = 0
    if len(max_heap) > len(min_heap):
        median_ = max_heap[0]
    else:
        median_ = (max_heap[0] - min_heap[0]) / 2.0
    median_ = myround(median_)
    record = (key[0], key[1], median_, cnt_, sum_)
    data_list.append(record)

df_zipcode_out = pd.DataFrame(data_list)
output_file = '../output/medianvals_by_zip2.txt'
output_file = output_filename_id_zip
df_zipcode_out.to_csv(output_file, sep='|', header=None, index=False)

end = time.time()
print('V2 (pandas) processing time: ' + str(round(end - start, 3)) + ' (seconds)')

