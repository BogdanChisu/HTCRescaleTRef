import csv
import multiprocessing as mp

from itertools import islice

from time import perf_counter

def get_number_of_cores():
    ncores = mp.cpu_count()
    return ncores

def get_number_of_lines(file_path):
    with open(file_path, "r") as f:
        read1 = csv.reader(f)
        row_count = sum(1 for row in read1)
        print(f"Number of lines to be processed is: {row_count}")
        n_processes = get_number_of_cores() // 2
        print(f"Number of processes is set to: {n_processes}")
        n_lines_per_process = (row_count - 1) // n_processes
        print(f"Number of lines per process: {n_lines_per_process}")
        line_intervals = {}
        intervals_start = 1
        for i in range(n_processes):
            line_intervals[i] = [intervals_start, intervals_start + n_lines_per_process - 1]
            intervals_start = line_intervals[i][1] + 1
            if i == n_processes - 1:
                line_intervals[i][1] = row_count
        print(f"Intervals are:\n{line_intervals}")
        return line_intervals

def define_file_name(file_path, temperature_to_set):
    return file_path[: -4] + '_' +str(temperature_to_set) + 'C.csv'

def define_file_name_rescaled(file_path, temperature_to_set):
    return define_file_name(file_path, temperature_to_set)[:-4] + '_rescaled' + '.csv'

def read_csv_file_via_generator(file_path, start_l, end_l):
    with open(file_path, "r")as csv_file:
        read_1 = csv.reader(csv_file)
        for row in islice(read_1, start_l, end_l):
            yield row

def prep_first_line(file_path):
    source_file = file_path[:-8] + file_path[-4:]
    print("source file is:", source_file)
    with open(source_file, "r") as csv_file:
        read_2 = csv.reader(csv_file)
        header = ''
        for row in read_2:
            header = row
            break
        header = ','.join(header)
        header += ',temperature' + '\n'
        return header

def prep_first_line_htc(file_path):
    source_file = file_path[:-13] + file_path[-4:]
    print("source file is:", source_file)
    with open(source_file, "r") as csv_file:
        read_2 = csv.reader(csv_file)
        header = ''
        for row in read_2:
            header = row
            break
        header = ','.join(header)
        header += '\n'
        return header

def write_1st_line(file_path):
    print(f"1st line argument {file_path}")
    line_text = prep_first_line(file_path)
    print("-"*40, "\n", "file header contains \n", line_text, "\n", "-"*60)
    with open(file_path, "w") as csv_file:
        csv_file.write(line_text)

def write_1st_line_htc(file_path):
    print(f"1st line argument {file_path}")
    line_text = prep_first_line_htc(file_path)
    print("-"*40, "\n", "file header contains \n", line_text, "\n", "-"*60)
    with open(file_path, "w") as csv_file:
        csv_file.write(line_text)

def start_write(file_path, temperature_to_set):
    destination_file = define_file_name(file_path, temperature_to_set)
    print(f"Destination file {destination_file}")
    write_1st_line(destination_file)

def start_write_htc(file_path, temperature_to_set):
    destination_file = define_file_name_rescaled(file_path, temperature_to_set)
    print(f"Destination file for htc 1st line {destination_file}")
    write_1st_line_htc(destination_file)

def temperature_set(file_path, temperature_to_set, sem:mp.Semaphore, start_l, end_l):
    with sem:
        destination_file = define_file_name(file_path, temperature_to_set)
        result_csv_reading = read_csv_file_via_generator(file_path, start_l, (end_l + 1))
        with open(destination_file, "a") as csv1:
            for el in result_csv_reading:
                el.append(str(temperature_to_set))
                csv1.write(','.join(el))
                csv1.write('\n')

def htc_rescale(file_path, temperature_to_set, sem:mp.Semaphore, start_l, end_l, enamel_thickness, resin_thickness, enamel_conductivity, resin_conductivity):
    with sem:
        destination_file = define_file_name_rescaled(file_path, temperature_to_set)
        source_file = file_path[:-4] + '_' + str(temperature_to_set) + 'C' + file_path[-4:]
        result_csv_reading = read_csv_file_via_generator(source_file, start_l, (end_l + 1))
        with open(destination_file, "a") as csv1:
            for el in result_csv_reading:
                htc = float(el[-2])
                htc_new = recalculate_HTC(htc, enamel_thickness, resin_thickness, enamel_conductivity, resin_conductivity)
                el[-2] = "{:e}".format(htc_new) # conversion to scientific notation
                csv1.write(','.join(el))
                csv1.write('\n')

def run_writers_htc(file_path, temperature_to_set):
    intervals = get_number_of_lines(file_path)
    n_processes = len(intervals)
    sem = mp.Semaphore(n_processes)
    processes = [mp.Process(target = htc_rescale, args = (file_path, temperature_to_set, sem, val[0], val[1],
                                                                enamel_thickness, resin_thickness, enamel_conductivity,
                                                                resin_conductivity)) for val in intervals.values()]
    for process in processes:
        process.start()
    for process in processes:
        process.join()

def run_writers(file_path, temperature_to_set):
    intervals = get_number_of_lines(file_path)
    n_processes = len(intervals)
    sem = mp.Semaphore(n_processes)
    processes = [mp.Process(target = temperature_set, args = (file_path, temperature_to_set, sem, val[0], val[1]))
                 for val in intervals.values()]
    for process in processes:
        process.start()
    for process in processes:
        process.join()

def recalculate_HTC(htc, enamel_thickness, resin_thickness, enamel_conductivity, resin_conductivity):
    """

    :param htc: heat transfer coefficient in [W/m^2/K]
    :param enamel_thickness: enamel thickness in [m]
    :param resin_thickness: resin thickness in [m]
    :param enamel_conductivity: enamel thermal conductivity in [W/m/K]
    :param resin_conductivity: resin thermal conductivity in [W/m/K]
    :return: htc_new = recalculated value of heat transfer coefficient as a function of insulator material property
    """
    htc_new = (htc * enamel_conductivity * resin_conductivity) / \
              (enamel_conductivity * resin_conductivity + htc * (resin_conductivity * enamel_thickness + \
                                                                 enamel_conductivity * resin_thickness))
    return htc_new

def sanity_check(file_path, temp):
    destination_file = define_file_name(file_path, temp)
    with open(destination_file, "r") as f:
        read1 = csv.reader(f)
        row_count = sum(1 for row in read1)
        print("-" * 60)
        print(f"Number of lines written is: {row_count}")


file_source = '/home/bogdan/PycharmProjects/work_files/PAGLK4-DE-20k.csv'
temperature_to_set = 90
enamel_thickness = 1.5e-4 # 0.15[mm]
resin_thickness = 3e-4 # 0.3[mm]
enamel_conductivity = 0.25 # [W/m/K]
resin_conductivity = 0.22 # [W/m/K]

if __name__ == "__main__":
    job_start = perf_counter()
    start_write(file_source, temperature_to_set)
    run_writers(file_source, temperature_to_set)
    start_write_htc(file_source, temperature_to_set)
    run_writers_htc(file_source, temperature_to_set)
    print(f"Duration: {(perf_counter() - job_start):.2f} [s]")
    # sanity_check(file_source, temperature_to_set)
# temperature_set(file_source, temperature_to_set)
# print(define_file_name_rescaled('PAGLK$-DE-20k.csv', 90))
# print(recalculate_HTC(300, 0.00015, 0.0003, 0.25, 0.22))