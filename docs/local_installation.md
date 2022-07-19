# Local Installation Guide

## Description
Repository provides scripts to process config files produced by [config-creator](https://github.com/sean-conkie/config-creator.git) to create deployment files.

## Requirements
- Python

## Installation Steps
- Create virtual environment
```shell
python -m venv .\venv
```
- Install requirements
```shell
python -m pip install -r ./requirements.txt
```

## Running the job build script
Start the virtual environment
```shell
.\venv\Scripts\activate
```
Then run command
```shell
python .\buildjobs.py
```

### Job build script config
Parameters can be provided for the job build script, primarily to define output directories.  Provide a `.json` with the parameters, only include parameters to be overriden.

|Parameter|Description|Default|
|---|---|---|
|`config`|Input path for config files to be procesed|`./bq_application/job/`|
|`log`|Output path for log files|Environment variable SYS_LOG or `./batch_application/logs/`|
|`dag`|Output path for dag files|`./dags/`|
|`dag_sql`|Output path for dag sql files|`./dags/sql/`|
|`batch_scr`|Output path for batch script files|Environment variable SYS_SCR or `./batch_application/scripts/scr/`|
|`batch_sql`|Output path for batch sql files|Environment variable SYS_SQL or `./batch_application/scripts/sql/`|
|`table_def_file`|Output path for table definition files|`./batch_application/table/`|
|`table_cfg`|Output path for object builder config files|`./batch_application/cfg/`|
|`debug_level`|Specify the desired log level (default: DEBUG).  This can be one of the following: 'CRITICAL', 'DEBUG', 'ERROR', 'FATAL','INFO','NOTSET', 'WARNING'|`DEBUG`|

Run script
```shell
python ./buildjobs.py --config=./job_params.json
```

### Validating config files
Script `validatedagconfig.py` can be used to validate that configs being processed confirm to a specified schema.

#### Parameters
|Parameter|Description|
|---|---|
|`config_directory`|Specify the location of the config file(s) which are to be validated.|
|`config_list`|A list of paths to files to be validated.|
|`log_level`|Specify the desired log level (default: DEBUG).  This can be one of the following: 'CRITICAL', 'DEBUG', 'ERROR', 'FATAL','INFO','NOTSET', 'WARNING'|
|`log_directory`|Specify the desired output directory for logs.  No dir means no log file will be output.|

Run validation
```shell
python validatedagconfig.py --config_directory=./cfg --log_level="ERROR"
```
