import csv
import io
import os
import zipfile
import time
import multiprocessing as mp
import queue

def processRow(q):
    # q, row = item[0], item[1]
    while True:
        try:
            row = q.get(True, 10)
            # print(row['LMK_KEY'], row['LODGEMENT_DATE'], row['TRANSACTION_TYPE'], row['TOTAL_FLOOR_AREA'], row['ADDRESS'], row['POSTCODE'])
            time.sleep(0.250)
        except queue.Empty:
            pass

the_queue = mp.Queue()
the_pool = mp.Pool(500, processRow, (the_queue,))

files=0
counter=0
zf = zipfile.ZipFile('all-domestic-certificates.zip') 
for filename in zf.namelist():
    if os.path.basename(filename) == "certificates.csv":
        print(filename)
        files += 1
        with zf.open(filename) as csvfile:
            items_file = io.TextIOWrapper(csvfile)
            reader = csv.DictReader(items_file, delimiter=',', quotechar='"')
            for row in reader:
                # processRow(row)
                counter += 1
                # print(counter)
                the_queue.put(row)
        print(counter)
        if files > 0:
            break

while True:
    qsize = the_queue.qsize()
    if qsize == 0:
        break
    time.sleep(1)
print(counter)
