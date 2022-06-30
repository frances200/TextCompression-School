import logging
from datetime import datetime
import gzip
import os
import time
from enum import Enum
import csv
import zlib
import bz2
import lzma

import threading
import queue

REMOVE_AFTER_BENCHMARK = True
time_string = datetime.now()
time_string = time_string.strftime("%c").replace(' ', '-').replace(':', '_')


global_lock = threading.Lock()
csv_rows = []


class Compression_types(str, Enum):
    gzip = 'gzip',
    zlib = 'zlib',
    bz2 = 'bz2',
    lzma = 'lzma'


def read_file(PATH):
    try:
        file = open(PATH)
        return file.read()
    except Exception as err:
        logging.error(err)


def write_csv_line(compression_type, compress, time, size, size_before):
    while global_lock.locked():
        continue
    global_lock.acquire()

    csv_rows.append([compression_type, compress, time, size, size_before])

    global_lock.release()


def compress_gzip(file, output_file):
    with gzip.open(output_file, 'wb') as f:
        f.write(file.encode())


def compress_zlib(file, output_file):
    output = zlib.compress(file.encode())
    with open(output_file, "wb") as f:
        f.write(output)


def compress_bz2(file, output_file):
    with bz2.open(output_file, "wb") as f:
        f.write(file.encode())


def compress_lzma(file, output_file):
    with lzma.open(output_file, "wb") as f:
        f.write(file.encode())


def decompress_gzip(file_path, output_file):
    with gzip.open(file_path, 'rb') as f:
        file = f.read()
        with open(output_file, 'wb') as o:
            o.write(file)


def decompress_zlib(file_path, output_file):
    with open(file_path, 'rb') as f:
        file = f.read()
        file = zlib.decompress(file)
        with open(output_file, 'wb') as o:
            o.write(file)


def decompress_bz2(file_path, output_file):
    with bz2.open(file_path, 'rb') as f:
        file = f.read()
        with open(output_file, 'wb') as o:
            o.write(file)


def decompress_lzma(file_path, output_file):
    with lzma.open(file_path, 'rb') as f:
        file = f.read()
        with open(output_file, 'wb') as o:
            o.write(file)


q = queue.Queue()


def worker(id):
    while True:
        item = q.get()
        benchmark_task(item, id)
        q.task_done()


def do_benchmark(file, compress_function, decompress_function, compression_type, size_before, id, file_extention='txt.gz'):
    output_compressed_file = f'compressed_files\\{compression_type}-{time_string}-{id}.{file_extention}'
    output_decompressed_file = f'decompressed_files\\{compression_type}-{time_string}-{id}.{file_extention}'

    # COMPRESSING BENCHMARK
    start_time = time.time()
    compress_function(file, output_compressed_file)
    exc_time = (time.time() - start_time)
    compressed_file_size = os.path.getsize(output_compressed_file)
    compression_result = [compression_type, True, exc_time,
                          compressed_file_size, size_before]
    write_csv_line(*compression_result)

    # DECOMPRESSING BENCHMARK
    start_time = time.time()
    decompress_function(output_compressed_file, output_decompressed_file)
    exc_time = (time.time() - start_time)
    decompressed_file_size = os.path.getsize(output_decompressed_file)
    decompression_result = [compression_type, False, exc_time,
                            decompressed_file_size, compressed_file_size]
    write_csv_line(*decompression_result)

    if(REMOVE_AFTER_BENCHMARK):
        os.remove(output_compressed_file)
        os.remove(output_decompressed_file)


def main():
    # Bestand inlezen
    all_files = []
    for file in os.listdir("files"):
        # check only text files
        if file.endswith('.txt'):
            all_files.append(file)

    # CREATE WORKERS
    for i in range(10):
        threading.Thread(target=worker, args=([i]), daemon=True).start()

    for file in all_files:
        q.put(file)

    q.join()

    with open(f'benchmarks\\benchmark_{time_string}.csv', 'w') as csv_file:
        csv_writer = csv_writer = csv.writer(csv_file)
        csv_writer.writerow(
            ['type', 'compression', 'exc_time', 'new_size', 'old_size'])
        for row in csv_rows:
            csv_writer.writerow(row)


def benchmark_task(file_name, id):
    logging.info(f'Doing benchmarks for file: {file_name}')
    PATH = f'files/{file_name}'
    file = read_file(PATH)
    file_size = os.path.getsize(PATH)

    # RUN THE BENCHMARKS
    do_benchmark(file, compress_gzip, decompress_gzip,
                 Compression_types.gzip, file_size, id)
    do_benchmark(file, compress_zlib, decompress_zlib,
                 Compression_types.zlib, file_size, id)
    do_benchmark(file, compress_bz2, decompress_bz2,
                 Compression_types.bz2, file_size, id)
    do_benchmark(file, compress_lzma, decompress_lzma,
                 Compression_types.lzma, file_size, id, 'xz')


if __name__ == "__main__":
    logging.basicConfig(format='%(levelname)s:\t%(message)s',
                        level=logging.DEBUG)
    main()



# program to compute the time
# of execution of any python code
import time
 
# we initialize the variable start
# to store the starting time of
# execution of program
start = time.time()
 
# we can take any program but for
# example we have taken the below
# program
a = 0
for i in range(1000):
    a += (i**100)
 
# now we have initialized the variable
# end to store the ending time after
# execution of program
end = time.time()
 
# difference of start and end variables
# gives the time of execution of the
# program in between
print("The time of execution of above program is :", end-start)