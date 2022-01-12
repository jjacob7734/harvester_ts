# harvester

Automatically harvest (identify, download and ingest) data granules from a
remote archive.  This capability is general, but is applied to various
wind datasets in this project.  As described below, parameters in a
configuration file are used by the automatic data harvester to resolve
paths to a dataset on a remote hosting site and to indicate local naming
conventions for the files.

## How to set up a dataset for harvesting

To harvest a dataset, do the following:

1. Create a directory to hold the data.
2. Inside that directory you just created, create a hidden
subdirectory called `.harvest`.
3. Inside that `.harvest` directory you just created, 
create a YAML configuration file called `dataset.yaml`.
The values in `dataset.yaml` may be constructed
using the string substitution templates for elements of date or time,
as described below.  You must define the following required keywords in
`dataset.yaml`:
    * `url_template`: Web address of remote data granules
    * `local_path_template`: Desired local path for data granules.  This
    should specify a relative path that will be automatically combined
    with the base directory specified with the `-b` option to the harvester
    script to produce an absolute path.  Similarly, if it is desired to
    upload the file to AWS S3 object store, then the `-B` option to the
    harvester script is used to indicate a prefix for the full path on S3.
    Whether the file path is on the local file system or on S3, this
    relative local path is (i) subjected to string substitution to specify the
    time stamp in the filename (see below), and (ii) is combined with the base
    directory specified as a command-line argument to produce an absolute
    path for the harvested file.
    * `time_res`: Time increment between data granules (see below)
4. Run `harvest.py` on the dataset directory (see example below)

## Template string substitution in the configuration

In the `url_template` and `local_path_template` settings the following
substrings may be used to indicate date- and time-specific string
substitutions as follows:

Template Substring | Granule Level Substitution
-------------------|----------------------------------------
`%Y` | 4-digit year in `{0000, 0001, ...}` (example: `2019`)
`%m` | 2-digit month in `{01, 02, ..., 12}`
`%d` | 2-digit day of month in `{01, 02, ...}`
`%H` | 2-digit hour in `{00, 01, ..., 23}`
`%M` | 2-digit minute in `{00, 01, ..., 59}`
`%S` | 2-digit second in `{00, 01, ..., 59}`
`%j` | 3-digit day of year in `{001, 002, ...}`

For the `time_res` setting, use an integer number of one of the
following units:

Template Substring | Units
-------------------|-------
`s` | seconds
`m` | minutes
`h` | hours
`d` | days
`w` | weeks

## Command line usage

```
usage: python3 harvest.py harvest.py [-h] [-s START_DATE] [-e END_DATE]
                  [-n NUM_DAYS] [-b DATA_BASEDIR] [-B S3_DATA_BASEDIR]
		  [-p PROFILE]

optional arguments:
  -h, --help            show this help message and exit
  -s START_DATE, --start_date START_DATE
                        start date to harvest (YYYYMMDD)
  -e END_DATE, --end_date END_DATE
                        end date to harvest (YYYYMMDD)
  -n NUM_DAYS, --num_days NUM_DAYS
                        number of days to harvest
  -b DATA_BASEDIR, --data_basedir DATA_BASEDIR
                        path to local dataset directory
  -B S3_DATA_BASEDIR, --s3_data_basedir S3_DATA_BASEDIR
                        path to local dataset folder on S3
  -p PROFILE, --profile PROFILE
                        profile for aws secret keys
```

Any sensible combination of the `-s`, `-e`, or `-n` parameters can be used
to specify the desirered date range to harvest:

If a `-b --data_basedir` argument is provided, then the data will be harvested
to the specified local path.  If a `-B --s3_data_basedir` argument is provide,
then the data will additionally be uploaded to the specified bucket/folder
on AWS S3.

### Example: Specify start and end dates:

These examples are intended to show the ways `-s`, `-e`, and `-n` options
can be to specify the dates to harvest to a local file system (with the `-b`
option used to indicate the base directory for the relative paths specified in
the `local_path_template` variable of the dataset configuration	(see above).
If upload to AWS S3 is desired (not shown in these examples), the `-B` and
`-p` argument may be used to specify the S3 file path prefix and AWS
credential profile to be used.

```
python3 harvest.py -s 20190921 -e 20190927 -b /data/mydata
```

### Example: Specify start date and number of days:

```
python3 harvest.py -s 20190921 -n 7 -b /data/mydata
```

### Example: Specify end date and number of days:

```
python3 harvest.py -e 20190927 -n 7 -b /data/mydata
```

### Example: Specify number of days ending today:

```
python3 harvest.py -n 7 -b /data/mydata
```

### Example: Specify just a single day (today):

```
python3 harvest.py -b /data/mydata
```
