#!/usr/bin/python3

"""Aggregate metrics files by time period, sorts the metrics by field and and prints the results to stdout."""
__author__ = 'Mark Howard'

import argparse
import datetime
from collections import namedtuple
from operator import attrgetter
import re
import signal


# the main metrics list
metrics = dict()
metrics_field_names = ('date', 'time', 'name', 'count', 'avg', 'max', 'sum', 'errcount')
MetricsTuple = namedtuple('MetricsTuple', metrics_field_names)

# list of fields to sort the loaded metrics by before display. The values will correspond to those listed
# in metrics_field_names
sort_fields = list()

# constant to divide the raw microsecond values by when displaying output. Forces the output to look like ms or seconds.
unit_divisor = 1

TIMING_RECORD='tm'

MICROSECONDS_TO_MILLISECONDS=1000
MICROSECONDS_TO_SECONDS=1000*1000

MILLISECONDS_TO_MINUTE = 60 * 1000
MILLISECONDS_TO_HOUR = 60 * 60 * 1000
MILLISECONDS_TO_DAY = 24 * 60 * 60 * 1000

def field_value(field_name, fields):
    """Pulls the first field from a list of fields.

    :param field_name field to find
    :param fields list of fields from the file (name=value strings)
    :return value as int, or zero if no field found
    """

    for field in fields:
        if field.startswith(field_name):
            return int(field[len(field_name) + 1:])

    return 0;


def record_metrics_line(ms, name, metrics_fields):
    """Saves a line in the metrics file to the internal dictionary of aggregated metrics.
    The dictionary is internally keyed by 'ms-name'. If a row already exists in the dictionary, the new
    values are accumulated with the existing. Internally, the dictionary values are represented as MetricsTuples.

    :param ms: millisecond value of the date/time of this line. Probably truncated to the aggregation time period.
    :param name: name of the class and function being measured
    :param metrics_fields: the data fields. In reality, all the fields in the metrics file after the initial time, type,
        and name
    """

    key = str(ms) + '-' + name

    existing = None
    if key in metrics:
        existing = metrics[key]

    count_value = 0 if existing is None else existing.count
    avg_value = 0 if existing is None else existing.avg
    max_value = 0 if existing is None else existing.max
    sum_value = 0 if existing is None else existing.sum
    err_count_value = 0 if existing is None else existing.errcount

    count_value += field_value('count', metrics_fields)
    max_value += field_value('max', metrics_fields)
    sum_value += field_value('sum', metrics_fields)
    err_count_value += field_value('err.count', metrics_fields)

    # re-calculate the average as the sum divided by the count. Don't just accumulate averages, as that will give you
    # a bogus, cumulative number
    avg_value = sum_value // count_value if count_value > 0 else 0

    dt = datetime.datetime.fromtimestamp(ms / 1000)
    date = dt.strftime('%Y-%m-%d')
    time = dt.strftime('%H:%M')

    mt = MetricsTuple(date, time, name, count_value, avg_value, max_value, sum_value, err_count_value)

    metrics[key] = mt


def parse_filename_date(filename):
    """Retrieve the filename date from a file in the format <productname>-<YYYYMMDD>.txt.
    Assumes the date is the 8 characters immediately before the file extension.

    :return the day the filename refers to, as a datetime.date object
    """

    extension_index = filename.rindex('.')
    iso_date = filename[extension_index - 8:extension_index]

    return datetime.datetime.strptime(iso_date, '%Y%m%d').date()


def read_file(filename, aggregation_func, regex_func):
    """Processes a single metrics file, importing its timing events into memory.

    :param filename: file to process
    :param aggregation_func: function to take a date and time and reduce it to the millisecond value denoting the
        beginning of a time aggregation period. Optional.
    :param: regex_func: function to perform a regex comparison on the 'name' field. Function is used to filter
        classes and functions into the output.
    """

    file_date = parse_filename_date(filename)

    with open(filename) as file:
        for line in file:
            split_line = line.split(', ')
            raw_time = split_line[0]
            type = split_line[1]
            name = split_line[2]
            summary_fields = split_line[3:]

            line_time = datetime.datetime.strptime(raw_time, "%H:%M:%S").time()
            ms = aggregation_func(file_date, line_time)

            if type == TIMING_RECORD:  # only for timing events
                if (regex_func is None) or (regex_func(name) == True):
                    record_metrics_line(ms, name, summary_fields)


def print_metrics(sorted_metrics):
    """Output the sorted metrics in a fixed-width format. This format is intended for console output and is not
    suitable for narrow display devices.
    """
    name_field_width = 70;

    for metrics in sorted_metrics:
        abbreviated_name = metrics.name if len(metrics.name) < name_field_width else '...' + metrics.name[-name_field_width+3:]

        print('{date} {time}  {name:70s}  count={count:4d}   sum={sum:7d}   max={max:7d}   avg={avg:7d}   '
              'errs={errcount:3d}'
              .format(date=metrics.date, time=metrics.time,
                      name=abbreviated_name, count=metrics.count,
                      sum=metrics.sum // unit_divisor, max=metrics.max // unit_divisor, avg=metrics.avg // unit_divisor,
                      errcount=metrics.errcount))


def process_files(files, aggregation_ms=None, grep_regex=None):
    """ Process a collection of metrics files, importing them into memory.
    """

    def calculate_aggregation_period_start_ms(d, t):
        """Takes the given date and time and returns a millisecond value pinned to the start of an aggregation
        time period.

        :param: d date ex filename
        :param: t time ex file line
        """

        # combine the date and time and reduce it to a millisecond value
        dt = datetime.datetime.combine(d, t)
        ms = int(dt.timestamp() * 1000)

        # if aggregation is enabled, trim the ms value to the beginning of the aggregation period.
        if not aggregation_ms is None:
            ms = (ms // aggregation_ms) * aggregation_ms

        return ms

    regex_func = None
    compiled_regex = None;

    def filter_by_regex(s):
        """Does this string match the compiled_regex?

        :param s: string to compare
        :return: True or False
        """
        return compiled_regex.search(s) is not None


    if grep_regex is not None:
        compiled_regex = re.compile(grep_regex, re.I)
        regex_func = filter_by_regex

    # read all files
    for filename in files:
        read_file(filename, aggregation_func=calculate_aggregation_period_start_ms, regex_func=regex_func)

    # sort
    sorted_metrics = sorted(metrics.values(), key=attrgetter(*sort_fields))

    # output
    print_metrics(sorted_metrics)


# main entry point
signal.signal(signal.SIGPIPE, signal.SIG_DFL) # prevent IOException if a stdout closes prematurely (eg with | head)


# parse arguments.
argument_parser = argparse.ArgumentParser(
    description='Parses metrics files, potentially: 1. filtering them; 2. aggregating them by time; 3.sorting the '
                'aggregated results. Prints the sorted, aggregated results to stdout.')

argument_parser.add_argument('--aggregate', '-a', metavar='<timeinverval>',
                             help='Aggregate metrics by this time interval. Time specifier is in the form 99[m|h|d] '
                                  'm=minutes, h=hours, d=days. eg 15m is 15 minutes.')
argument_parser.add_argument('--sort', '-s', metavar='<sortfields>',
                             default="date,time,name",
                             help='Sort results. Comma-separated list of {}'.format(metrics_field_names))
argument_parser.add_argument('--units', '-u', choices=['us', 'ms', 'sec'], default='ns',
                             help='display units for output')
argument_parser.add_argument('--grep', '-g', '--regex', metavar='<regex>',
                             help='Only process metrics with the class/method name matching a regular expression')
argument_parser.add_argument('files', metavar='file', nargs="+",
                             help="Files must be named <prefix>-YYYYMMDD.<ext>. The date stamp must be 8 characters,"
                                  "immediately before the file extension")

args = argument_parser.parse_args()


# miscellaneous argument parsing

# get the number of milliseconds to aggregate by
milliseconds_aggregation = None;
if args.aggregate is not None:

    # format: 99[m|h|d]
    quantity = int(args.aggregate[0:-1])
    units_specifier = args.aggregate[-1:].upper()
    if units_specifier == 'M':  # minutes
        milliseconds_aggregation = quantity * MILLISECONDS_TO_MINUTE
    elif units_specifier == 'H':  # hours
        milliseconds_aggregation = quantity * MILLISECONDS_TO_HOUR
    elif units_specifier == 'D':  # days
        milliseconds_aggregation = quantity * MILLISECONDS_TO_DAY
    else:
        argument_parser.exit('--aggregate fields must specify a time range in the format 999[m|h|d]. '
                             'm=minute, h=hour, d=day')


# parse the --sort fields array. Terminate if an illegal field is present
if args.sort is not None:
    args_sort_fields = args.sort.split(",")
    for field in args_sort_fields:
        sort_fields.append(field.strip())
        if field.strip() not in metrics_field_names:
            argument_parser.exit('--sort fields must be a comma separated list of values. '
                                 'Valid values are: {metrics_field_names}'.format(metrics_field_names=metrics_field_names))

# parse the --units argument.
if args.units == 'ms':
    unit_divisor = MICROSECONDS_TO_MILLISECONDS;
elif args.units == 'sec':
   unit_divisor = MICROSECONDS_TO_SECONDS;

process_files(args.files, aggregation_ms=milliseconds_aggregation, grep_regex=args.grep)
