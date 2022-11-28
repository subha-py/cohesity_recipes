# step: 1 discover files in folder /home/cohesity/FioScripts with *fio
# step: 2 run fio in parallel

from multiprocessing import Pool, cpu_count
import time
import random
import os
import subprocess

def get_files_with_extension(directory, extension=None):
    files = os.listdir(directory)
    result = []
    for file in files:
        if extension is not None and not file.endswith(extension):
            continue
        result.append(os.path.join(directory, file))
    return result


def get_fio_jobs(path="/home/cohesity/FioScripts"):
    return get_files_with_extension(path, '.job')

def get_fio_files(path="/home/cohesity/FioFiles"):
   return get_files_with_extension(path)

def run_fio_command(fio_job_file):
    process = subprocess.Popen(['fio', fio_job_file],
                               stdout=subprocess.PIPE,
                               universal_newlines=True)

    while True:
        output = process.stdout.readline()
        print(output.strip())
        # Do something else
        return_code = process.poll()
        if return_code is not None:
            print('RETURN CODE', return_code)
            # Process has finished, read rest of the output
            for output in process.stdout.readlines():
                print(output.strip())
            break

def run_fio_jobs_in_parallel(fio_files, count=0):
    pool = Pool(processes=cpu_count()-1)
    for arg in fio_files:
        print(arg)
        pool.apply_async(run_fio_command, args=(arg,))
    pool.close()
    pool.join()
    print("Count - {}".format(count))
    return

if __name__ == '__main__':
    count = 0
    while True:
        run_fio_jobs_in_parallel(get_fio_jobs(),count=count)
        count += 1