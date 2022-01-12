import sys, os
import argparse
from datetime import datetime, timezone, timedelta
import logging
import yaml
from subprocess import run
from netCDF4 import Dataset
import boto3


def parse_args():
    """Retrieve command line parameters.
    
    Returns:
        ArgumentParse: command line parameters
    """
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("-b", "--basedir",
                        help="path to local dataset directory")
    parser.add_argument("-B", "--s3_basedir",
                        help="path to local dataset folder on S3")
    parser.add_argument("-s", "--start_date",
                        help="start date to harvest (YYYYMMDD)")
    parser.add_argument("-e", "--end_date",
                        help="end date to harvest (YYYYMMDD)")
    parser.add_argument("-n", "--num_days", type=int,
                        help="number of days to harvest")
    parser.add_argument("-p", "--profile",
                        help="profile for aws secret keys")
    args = parser.parse_args()
    return args

def read_dataset_conf(conf_fname, logger=None):
    """Read YAML config file.

    Args:
        conf_fname (str): Name of YAML configuration file.
        logger (logger): Logger object

    Returns:
        dict: Configuration parameters from the configuration file.
    """
    if os.path.isfile(conf_fname):
        with open(conf_fname, 'r') as f:
            try:
                conf = yaml.safe_load(f)
            except yaml.YAMLError as exc:
                print(exc)
    else:
        if logger:
            logger.error("{} does not exist.".format(conf_fname))
        sys.exit(5)
    return conf

def set_date_range(args, date_fmt="%Y%m%d", logger=None):
    """Determine desired start and end times to harvest from input parameters.

    Args:
        args (ArgumentParser): command line parameters
        date_fmt (str): Format of date part of times
        logger (logger): Logger object

    Returns:
        datetime: Start date to harvest.
        datetime: End date to harvest.
    """
    # Check validity of supplied arguments
    utcnow = datetime.utcnow()
    utc_today = datetime(utcnow.year, utcnow.month, utcnow.day,
                         tzinfo=timezone.utc)
    if args.get('start_date'):
        start_date_in = datetime.strptime(args.get('start_date'), date_fmt)
        start_date = datetime(start_date_in.year, start_date_in.month,
                              start_date_in.day, start_date_in.hour, 0, 0, tzinfo=timezone.utc)
        if start_date > utc_today:
            if logger is not None:
                logger.error("Cannot specify a start date in the future")
            sys.exit(1)
    if args.get('end_date'):
        end_date_in = datetime.strptime(args.get('end_date'), date_fmt)
        end_date = datetime(end_date_in.year, end_date_in.month,
                            end_date_in.day, end_date_in.hour, 59, 59, tzinfo=timezone.utc)
        if end_date < start_date:
            if logger is not None:
                logger.error("End date cannot be before start date.")
            sys.exit(2)
    if args.get('num_days') and args.get('num_days') < 1:
        if logger is not None:
            logger.error("Cannot specify less than 1 days to harvest")
        sys.exit(3)
        
    # Determine start and end dates to harvest from supplied arguments
    if args.get('num_days'):
        ndays = timedelta(days=args.get('num_days')) - timedelta(seconds=1)
        if args.get('start_date') and args.get('end_date'):
            # User improperly specified: -n N -s YYYYMMDD -e YYYYMMDD
            if logger is not None:
                logger.error("Cannot specify all 3 of start date, end date and number of days")
            sys.exit(4)
        elif args.get('start_date'):
            # User specified: -n N -s YYYYMMDD; calculate end_date
            end_date = start_date + ndays
        elif args.get('end_date'):
            # User specified: -n N -e YYYYMMDD; calculate start_date
            start_date = end_date - ndays
        else:
            # User specified: -n N
            # Only num_days specified; defaulting to that many days
            # ending on today.
            end_date = datetime(utc_today.year, utc_today.month, utc_today.day,
                                23, 59, 59, tzinfo=timezone.utc)
            start_date = end_date - ndays
    else:
        if args.get('start_date') and args.get('end_date'):
            # User specified -s YYYYMMDD -e YYYYMMDD; nothing more needs to
            # be done.
            pass
        elif args.get('start_date'):
            # User specified -s YYYYMMDD; set end date to today
            end_date = datetime(utc_today.year, utc_today.month, utc_today.day,
                                23, 59, 59, tzinfo=timezone.utc)
        elif args.get('end_date'):
            # User specified -e YYYYMMDD; set start date same as end date
            start_date = end_date
        else:
            # User specified no arguments; harvest just for today
            start_date = utc_today
            end_date = datetime(utc_today.year, utc_today.month, utc_today.day,
                                23, 59, 59, tzinfo=timezone.utc)
    return start_date, end_date

def replace_template(template, cur_date):
    """Replace format strings in template with appropriate date fields.

    Args:
        template (str): Template string with format identifiers
        cur_date (datetime): Date being harvested

    Returns:
        str: Template string with format substrings replaced by the
             appropriate fields in the specified date.
    """
    trans_key = {"%Y": "{:04d}".format(cur_date.year),
                 "%m": "{:02d}".format(cur_date.month),
                 "%d": "{:02d}".format(cur_date.day),
                 "%H": "{:02d}".format(cur_date.hour),
                 "%M": "{:02d}".format(cur_date.minute),
                 "%S": "{:02d}".format(cur_date.second),
                 "%j": "{:03d}".format(cur_date.timetuple().tm_yday)}
    formatted_str = template
    for key, val in trans_key.items():
        formatted_str = formatted_str.replace(key, val)
    return formatted_str

def time_setting_dict(time_str):
    """Convert a time string like 90s, 3h, 1d, to a dictionary with a
    single keyword-value pair appropriate for keyword value settings to
    the python datetime.timedelta function.  For example, input of "3h"
    should return {"hours": 3}.

    Args:
        time_str: Time string with units (e.g., 90s, 3h, 1d)

    Returns:
        dict: Keyword-value pair indicating command line arguments
    """
    time_unit_dict = {"s": "seconds",
                      "m": "minutes",
                      "h": "hours",
                      "d": "days",
                      "w": "weeks"}
    return {time_unit_dict[time_str[-1]]: int(time_str[:-1])}

def paths_generator(start_date, end_date, local_basedir, dataset_conf):
    """Generator that yields remote url, local directory and local path for
    the download.

    Args:
        start_date (datetime): Start date/time to harvest
        end_date (datetime): End date/time to harvest
        local_basedir (str): Directory to download to
        dataset_conf (dict): Dataset configuration dictionary

    Yields:
        str: Remote url
        str: Local file name
    """
    time_res = dataset_conf["time_res"]
    time_incr = timedelta(**time_setting_dict(time_res))
    cur_date = start_date
    while cur_date <= end_date:
        local_fname = replace_template(dataset_conf["local_path_template"],
                                       cur_date)
        url = replace_template(dataset_conf["url_template"], cur_date)
        local_path = os.path.join(local_basedir, local_fname)
        yield url, local_path, local_fname
        cur_date += time_incr

def upload_to_s3(local_path, s3_path, s3_profile):
    s3_path_split = s3_path[5:].split('/')
    s3_bucket = s3_path_split[0]
    s3_key = os.path.join(*s3_path_split[1:])
    s3client = boto3.Session(profile_name=s3_profile).client('s3')
    s3client.upload_file(local_path, s3_bucket, s3_key)

def harvest_date_range(start_date, end_date, local_basedir,
                       dataset_conf, hidden_dirpath, tmp_dirpath, 
                       s3_basedir=None, s3_profile=None, logger=None):
    """Retrieve granules in the specified time range.

    Args:
        start_date (datetime): Start date/time to harvest
        end_date (datetime): End date/time to harvest
        local_basedir (str): Directory to download to
        dataset_conf (dict): Dataset configuration dictionary
        logger (logger): Logger object
    """
    if not os.path.exists(local_basedir):
        raise OSError("Local base directory {} must be created.".\
                      format(local_basedir))
    if not os.path.exists(hidden_dirpath):
        raise OSError("Hidden directory {} must be created.".\
                      format(hidden_dirpath))
        
    for url, local_path, local_fname in paths_generator(start_date, end_date,
                                                        local_basedir,
                                                        dataset_conf):
        local_dir = os.path.dirname(local_path)
        base_fname = os.path.basename(local_path)
        tmp_fname = os.path.join(tmp_dirpath, base_fname)
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)
        if not os.path.exists(local_path):
            run(["wget", url, "-q", "-O", tmp_fname])
            try:
                rootgrp = Dataset(tmp_fname, "r", format="NETCDF4")
            except:
                if os.path.isfile(tmp_fname):
                    os.remove(tmp_fname)
                logger.error("Unable to download {}".format(url))
            else:
                os.rename(tmp_fname, local_path)
                logger.warning("Downloaded {} to {}".format(url, local_path))
                if s3_basedir is not None:
                    s3_path = os.path.join(s3_basedir, local_fname)
                    upload_to_s3(local_path, s3_path, s3_profile)
                    logger.warning("Uploaded to {}".format(s3_path))
  
def main():
    """Main program.  Parse arguments, and harvest the requested dates from
    the remote archive.
    """
    # Initializer logger
    logger = logging.getLogger()
    args = {}

    # Parse arguments and set date range to harvest
    args = vars(parse_args()) # convert namespace object to dict

    # Set base directory for harvested data.
    local_basedir = args.get('basedir')
    s3_basedir = args.get('s3_basedir')
    s3_profile = args.get("profile")

    # Set start/end dates
    date_fmt_precise = "%Y-%m-%dT%H:%M:%SZ"
    date_fmt = "%Y%m%d"
    start_date, end_date = set_date_range(args, date_fmt=date_fmt, logger=logger)
    start_date_str = start_date.strftime(date_fmt_precise)
    end_date_str = end_date.strftime(date_fmt_precise)
    logger.info("Harvesting between {} and {} to {}".format(start_date_str,
                                                            end_date_str,
                                                            local_basedir))
   
    # Read dataset configuration
    hidden_dirname = ".harvest"
    conf_fname = "dataset.yaml"
    tmp_dirname = "tmp"
    tmp_dirpath = os.path.join(os.path.dirname(local_basedir),
                               tmp_dirname,
                               os.path.basename(local_basedir))
    hidden_dirpath = os.path.join(local_basedir, hidden_dirname)
    conf_fname = os.path.join(hidden_dirpath, conf_fname)
    dataset_conf = read_dataset_conf(conf_fname, logger=logger)
    
    # Harvest data for the specified date range from the remote archive.
    harvest_date_range(start_date, end_date, local_basedir, dataset_conf, 
                       hidden_dirpath, tmp_dirpath, s3_basedir=s3_basedir, 
                       s3_profile=s3_profile, logger=logger)


if __name__ == "__main__":
    main()
