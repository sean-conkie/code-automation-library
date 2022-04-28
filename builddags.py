import inspect
import json
import os
import re
import sys

from datetime import datetime, timedelta
from venv import create
from jinja2 import Environment, FileSystemLoader

global LOGGING_MODE
global LOGGING_VERBOSE
LOGGING_MODE='DEBUG'
LOGGING_VERBOSE = True

def main(args = {}):
    '''Main method for generating .py files containing DAGs from config files.
    
    From JSON config file(s) supplied as a dir or file, generate a .py file
    using template_dag.txt.  Loops through all configs provided and creates 
    one .py file per config.

    args:
        args: A dictionary of input parameters, defaults to empty dictionary.
              Dictionary can contain one or more key value paris of;
                --dir: source file path or directory containing config files
                --out-dir: target output file path for .py files to be saved

    returns:
        Saves .py files in output directory and returns;
            0: succesful
            1: failure'''

    log(f'dag files STARTED',"IMPORTANT")
    dpath = "./cfg/dag/" if '--dir' not in args.keys() else args['--dir'] # set directory path to default if none provided
    opath = "./dags/" if '--out-dir' not in args.keys() else args['--out-dir'] # set output path to default if none provided
    config_list = []

    # create a list of config files using the source directory (dpath), if the 
    # path provided is a file add id otherwise append each filename in directory
    try:
        log(f'creating config list',"INFO")
        if not os.path.isdir(dpath) and os.path.exists(dpath):
            config_list.append(dpath)
        else:
            for filename in os.listdir(dpath):
                log(f'filename: {filename}',"INFO")
                m = re.search(r'^cfg_.*\.json$',filename,re.IGNORECASE)
                if m:
                    config_list.append(filename)
    except:
        log(f'{sys.exc_info()[0]:}',"ERROR")
        log(f'dag files FAILED',"INFO")
        return 1

    # for each config file identified use the content of the JSON to create
    # the python statements needed to be inserted into the template
    for config in config_list:
        path = config if os.path.exists(config) else f'{dpath}{config}'
        cfg = get_config(path)
        log(f"building dag - {cfg['dag']['name']}","INFO")

        dag_string = create_dag_string(cfg['dag'])
        default_args = create_dag_args(cfg['dag']['args'])
        imports = '\n'.join(cfg['imports'])
        tasks = []
        dependencies = []

        # for each item in the task array, check the operator type and use this
        # to determine the task parameters to be used
        for task in cfg['tasks']:
            log(f'creating task "{task["task_id"]}"',"INFO")
            if task['operator'] == 'CreateTable':
                task['parameters'] = create_table_task(task,cfg["dag"]["properties"])
                task['operator'] = 'BigQueryOperator'
            
            tasks.append(create_task(task))

            if 'dependencies' in task.keys():
                if len(task['dependencies']) > 0:
                    for dep in task['dependencies']:
                        dependencies.append(f"{dep} >> {task['task_id']}")
                else:
                    dependencies.append(f"start_pipeline >> {task['task_id']}")
                
        dep_tasks = [d[0].strip() for d in [dep.split('>') for dep in dependencies]]
        final_tasks = [task['task_id'] for task in cfg['tasks'] if not task['task_id'] in dep_tasks]

        for task in final_tasks:
            dependencies.append(f"{task} >> finish_pipeline")

        properties = []
        for key in cfg["dag"]["properties"].keys():
            value = cfg["dag"]["properties"][key]
            properties.append(f"{key} = '{value}'")

        log(f'populating template',"INFO")
        file_loader = FileSystemLoader('./templates')
        env = Environment(loader=file_loader)
    
        template = env.get_template('template_dag.txt')
        output = template.render(imports=imports, tasks=tasks, default_args=default_args, dag_string=dag_string, dependencies=dependencies, properties=properties)
        
        dag_file = f"{opath}{cfg['dag']['name']}.py"
        with open(dag_file,'w') as outfile:
            outfile.write(output)

    log(f'dag files COMPLETED SUCCESSFULLY',"IMPORTANT")
    return 0

def create_table_task(task, properties):
    '''Method for generating parameters dictionary for standard create table sql.
    
    From 

    args:
        task: A dictionary of representing a task to be added to the DAG.  Used to 
              task parameter string
        properties: DAG properties.  Used to obtain DAG level properties, such as
                    the staging dataset

    returns:
        A dictionary containing expected parameters for the desired task (BigQueryOperator)
    '''

    log(f'STARTED',"INFO")
    dataset_staging = properties['dataset_staging']
    dataset_publish = '{dataset_publish}' if not 'destination_dataset' in task['parameters'].keys() else task['parameters']['destination_dataset'] # set to use variable from target file if not in task parameters
    destination_dataset_table = f"{dataset_publish}.{task['parameters']['destination_table']}"
    sql = task['parameters']['sql'] if 'sql' in task['parameters'].keys() else f'{create_sql(task, dataset_staging)}' # if user has provided a link to a .sql file, use it otherwise look to create sql from source to target parameter
    write_disposition = 'WRITE_TRUNCATE' if not 'write_disposition' in task['parameters'].keys() else f"{task['parameters']['write_disposition']}"

    outp = {
        'sql': sql,
        'destination_dataset_table': destination_dataset_table,
        'write_disposition': write_disposition,
        'create_disposition': 'CREATE_IF_NEEDED',
        'allow_large_results': True,
        'use_legacy_sql': False
    }

    log(f'COMPLETED SUCCESSFULLY',"INFO")
    return outp

def create_sql(task, dataset_staging=None):
    '''
    '''
    log(f'STARTED',"INFO")
    sql = []
    target_dataset = '{dataset_publish}' if 'destination_dataset' not in task['parameters'].keys() else task['parameters']['destination_dataset']

    log(f'creating sql for table type {task["parameters"]["target_type"]}',"INFO")
    if task['parameters']['target_type'] == 1:
        
        log(f'set write disposition - "{task["parameters"]["write_disposition"]}"',"INFO")
        if task['parameters']['write_disposition'] == 'WRITE_TRUNCATE':
            sql.append(f"truncate table {target_dataset}.{task['parameters']['destination_table']};")

        sql.append(f"insert into {target_dataset}.{task['parameters']['destination_table']}")

        r = create_sql_conditions(task)
        tables = r['tables']
        frm = r['from']
        where = r['where']

        select = create_sql_select(task,tables)

        sql.append(',\n'.join(select))
        sql.append('\n'.join(frm))
        sql.append('\n'.join(where))
        sql.append(';\n')

    elif task['parameters']['target_type'] == 2:
        td_table = re.sub(r'^[a-zA-Z]+_','td_',task['parameters']['destination_table'])
        log(f'create sql for transient table, pull source data and previous columns - "{dataset_staging}.{td_table}_p1"',"INFO")
        sql.append(f"create or replace table {dataset_staging}.{td_table}_p1 as")

        r = create_sql_conditions(task)
        tables = r['tables']
        frm = r['from']
        where = r['where']

        select = create_sql_select(task,tables)
        
        history = task['parameters']['history']
        log(f"setting history parameters","INFO")
        partition_list = []
        for p in history['partition']:
            partition_list.append(f'{p["source_name"]}.{p["source_column"]}' if 'source_column' in p.keys() else f'{p["transformation"]}')
        partition = ','.join(partition_list)
        
        order_list = []
        for p in history['order']:
            order_list.append(f'{p["source_name"]}.{p["source_column"]}' if 'source_column' in p.keys() else f'{p["transformation"]}')
        order = ','.join(order_list)

        log(f"add previous fields for {history['driving_column']}","INFO")
        prev_task = {'parameters': {'source_to_target': []}}
        prev_conditions = []
        for col in history['driving_column']:
            cj = {"name": f"prev_{col['name']}", "transformation": f"lag({col['source_name']}.{col['source_column']},1) over(partition by {partition} order by {order})"} 
            prev_task['parameters']['source_to_target'].append(cj)
            prev_conditions.append({
                "operator": "<>",
                "fields": [f"ifnull(cast({col['name']} as string),'NULL')",f"ifnull(cast(prev_{col['name']} as string),'NULL')"]
            })

        prev_select = create_sql_select(prev_task,tables)
        prev_select[0] = prev_select[0].replace('select','      ')
        for ps in prev_select:
            select.append(ps)

        sql.append(',\n'.join(select))
        sql.append('\n'.join(frm))
        sql.append('\n'.join(where))
        sql.append(';\n')

        log(f'create sql for transient table, complete CDC - "{dataset_staging}.{td_table}_p2"',"INFO")
        sql.append(f"create or replace table {dataset_staging}.{td_table}_p2 as")

        select = f"select * except({','.join([t['name'] for t in prev_task['parameters']['source_to_target']])})"
        tables[f"{dataset_staging}.{td_table}_p1"] = chr(len(tables.keys()) + 97)
        frm = f"  from {dataset_staging}.{td_table}_p1 {tables[f'{dataset_staging}.{td_table}_p1']}"
        
        where = create_sql_where(prev_conditions)

        sql.append(select)
        sql.append(frm)
        sql.append('\n'.join(where))
        sql.append(';\n')

        log(f'create sql for transient table, add/replace effective_to_dt with lead - "{dataset_staging}.{td_table}"',"INFO")
        
        log(f'set write disposition - "{task["parameters"]["write_disposition"]}"',"INFO")
        if task['parameters']['write_disposition'] == 'WRITE_TRUNCATE':
            sql.append(f"truncate table {target_dataset}.{task['parameters']['destination_table']};")
            
        sql.append(f"insert into {target_dataset}.{task['parameters']['destination_table']}")

        log(f"re-calculating history parameters","INFO")
        partition_list = []
        for p in history['partition']:
            partition_list.append(f'{p["name"]}')
        partition = ','.join(partition_list)
        
        order_list = []
        for p in history['order']:
            order_list.append(f'{p["name"]}')
        order = ','.join(order_list)

        new_source_to_target = {'parameters': {'source_to_target': []}}
        for s in task['parameters']['source_to_target']:
            if s['name'] == 'effective_to_dt':
                ns = {"name": s['name'], "transformation": f"lead(effective_from_dt,1,timestamp('2999-12-31 23:59:59')) over(partition by  {partition} order by {order})"} 
            else:
                ns = {
                    'name': s['name'],
                    'source_name': f"{dataset_staging}.{td_table}_p2", 
                    'source_column': s['name']
                }
            new_source_to_target['parameters']['source_to_target'].append(ns)

        tables[f"{dataset_staging}.{td_table}_p2"] = chr(len(tables.keys()) + 97)
        select = create_sql_select(new_source_to_target,tables)
        frm = f"  from {dataset_staging}.{td_table}_p2 {tables[f'{dataset_staging}.{td_table}_p2']}"
        
        sql.append(',\n'.join(select))
        sql.append(frm)
        sql.append(';\n')

    outp = '\n'.join(sql)
    log(f'COMPLETED SUCCESSFULLY',"INFO")
    return outp

def create_sql_select(task,tables):
    '''
    
    '''
    log(f'STARTED',"INFO")
    log(f'creating select list',"INFO")
    select = []
    for i,column in enumerate(task['parameters']['source_to_target']):
        prefix = "select " if i == 0 else "       "
        if 'source_name' in column.keys() and 'source_column' in column.keys():
            source = f"{tables[column['source_name']]}.{column['source_column']}" 
        else:
            transformation = column['transformation']
            for key in tables.keys():
                transformation = transformation.replace(key, tables[key])

            source = transformation

        if not 'source_column' in column.keys():
            column['source_column'] = ''

        alias = column['name'].rjust(max((60 - len(f'{prefix}{source}') + len(column["name"])) - 1,1 + len(column["name"]))) if not column['name'] == column['source_column'] else ''

        select.append(f"{prefix}{source}{alias}")
        
    log(f'COMPLETED SUCCESSFULLY',"INFO")
    return select

def create_sql_conditions(task):
    ''''''
    log(f'STARTED',"INFO")
    tables = {task['parameters']['driving_table']: "a"}
    i = 1
    frm = [f"  from {task['parameters']['driving_table']} {tables[task['parameters']['driving_table']]}"]
    log(f'identifying join conditions',"INFO")
    if 'joins' in task['parameters'].keys():
        for join in task['parameters']['joins']:
            if 'left' in join.keys():
                if not join['left'] in tables.keys():
                    tables[join['right']] = chr(i + 97)
                    i=+1
            if not join['right'] in tables.keys():
                tables[join['right']] = chr(i + 97)
                i=+1

            join_type = 'left' if not 'type' in join.keys() else join['type']
            left_table = task['parameters']['driving_table'] if not 'left' in join.keys() else join['left']
            right_table = join['right']
            frm.append(f"{join_type.rjust(6)} join {join['right']} {tables[join['right']]}")
            for j,condition in enumerate(join['on']):
                on_prefix = "(    " if len(join['on']) > 1 and j == 0 else ""
                on_suffix = ")" if len(join['on']) == j + 1 else ""
                prefix = "    on " if j == 0 else "        and "
                
                left = f"{condition['fields'][0].replace(left_table,f'{tables[left_table]}').replace(right_table,f'{tables[right_table]}')}"
                right = f"{condition['fields'][1].replace(left_table,f'{tables[left_table]}').replace(right_table,f'{tables[right_table]}')}"
                frm.append(f"{prefix}{on_prefix}{left} {condition['operator']} {right}{on_suffix}")
    else:
        left_table = task['parameters']['driving_table']
        right_table = ''


    where = create_sql_where(task['parameters']['where'], tables, left_table, right_table) if 'where' in task['parameters'].keys() else ''

    outp = {
        'tables': tables,
        'from': frm,
        'where': where
    }
    
    log(f'COMPLETED SUCCESSFULLY',"INFO")
    return outp

def create_sql_where(conditions, tables={}, left_table='', right_table=''):
    ''''''
    log(f'STARTED',"INFO")
    log(f'creating where conditions:',"INFO")
    log(f'               conditions  - {conditions}',"INFO")
    log(f'               tables      - {tables}',"INFO")
    log(f'               left_table  - {left_table}',"INFO")
    log(f'               right_table - {right_table}',"INFO")
    
    where = []
    for i,condition in enumerate(conditions):
        prefix = " where " if i == 0 else "   and "
        
        left = f"{condition['fields'][0].replace(left_table,f'{tables[left_table] if left_table in tables.keys() else left_table}').replace(right_table,f'{tables[right_table] if right_table in tables.keys() else right_table}')}"
        right = f"{condition['fields'][1].replace(left_table,f'{tables[left_table] if left_table in tables.keys() else left_table}').replace(right_table,f'{tables[right_table] if right_table in tables.keys() else right_table}')}"
        where.append(f"{prefix}{left} {condition['operator']} {right}")
    
    log(f'COMPLETED SUCCESSFULLY',"INFO")
    return where

def create_task(task):
    '''Method for generating a string of python that defines a task. 

    args:
        task: A dictionary representing a task to be added to the DAG.  Used to 
              task parameter string

    returns:
        A string of python code that can be added to the target file
    '''
    log(f'STARTED',"INFO")
    
    log(f'creating task {task["task_id"]} from:',"INFO")
    log(f'                           parameters - {task["parameters"]}',"INFO")

    outp = [f"{task['task_id']} = {task['operator']}(task_id='{task['task_id']}'"]

    # for each key:value pair in the tark parameters we perform checks based on 
    # parameter type and create a value that can be appended to the string
    for key in task['parameters'].keys():
        if type(task['parameters'][key]) == int or type(task['parameters'][key]) == bool:
            value = task['parameters'][key]
        elif type(task['parameters'][key]) == str:
            value = f"f'''{task['parameters'][key]}'''"

        outp.append(f"{key} = {value}")
    outp.append('dag=dag)')

    log(f'COMPLETED SUCCESSFULLY',"INFO")
    return ',\n          '.join(outp)

def create_dag_string(dag):
    '''Method for generating a string of python that defines a dag. 

    DAG parameters are provided and used to populate a string which can be
    added to the target file.

    args:
        dag: A dictionary representing the DAG.  Used to create dag string

    returns:
        A string of python code that can be added to the target file
    '''
    log(f'STARTED',"INFO")
    # we first set DAG defaults - these can also be excluded completely and
    # use Environment settings
    odag = {
        'concurrency': 10,
        'max_active_runs': 1,
        'default_args': 'default_args',
        'schedule_interval':None,
        'start_date': 'datetime.now()',
        'catchup': False
    }

    for arg in ['concurrency','max_active_runs']:
        if arg in dag.keys():
            if type(dag[arg]) == int:
                odag[arg] = dag[arg]

    if 'catchup' in dag.keys() and type(dag['catchup']) == bool: odag[arg] = dag[arg]

    if 'tags' in dag.keys():
        if type(dag['tags']) == list:
            odag['tags'] = dag['tags']
        else: 
            odag['tags'] = [dag['tags']]

    odag['description'] = f'"{dag["description"] if "description" in dag.keys() else dag["name"]}"'

    outp = f"'{dag['name']}',{', '.join([f'{key} = {odag[key]}' for key in odag.keys()])}"

    log(f'COMPLETED SUCCESSFULLY',"INFO")
    return outp

def create_dag_args(args):
    '''
    '''
    log(f'STARTED',"INFO")
    default_args = []

    for key in args.keys():
        if key in ['depends_on_past','email_on_failure','email_on_retry','wait_for_downstream']:
            value = False if not type(args[key]) == bool else args[key]
            default_args.append(f"'{key}': {value}")
        elif key in ['retry_delay','sla','execution_timeout']:
            if type(args[key]) == int:
                default_args.append(f"'{key}': timedelta(seconds={args[key]})")
        elif key in ['email']:
            emails = ','.join([f"'{a}'" for a in args[key]])
            default_args.append(f"'{key}': [{emails}]")
        elif not args[key] == "":
            default_args.append(f"'{key}': '{args[key]}'")

    outp = f"{{{','.join(default_args)}}}"

    log(f'COMPLETED SUCCESSFULLY',"INFO")
    return outp

def get_config(path):
    '''Method to load config file and retun dictionary object

    args:


    returns:
        dictionary object
    
    '''
    
    log(f'STARTED',"INFO")
    if not path:
        log(f'File {path:} does not exist.',"WARNING")
        return

    try:
        # identify what path is; dir, file 
        if os.path.isdir(path) or not os.path.exists(path):
            raise FileExistsError
    except (FileNotFoundError, FileExistsError) as e:
        log(f'File {path:} does not exist.',"ERROR")
        log(f'FAILED',"INFO")
        return
    except:
        log(f'{sys.exc_info()[0]:}',"ERROR")
        log(f'FAILED',"INFO")
        return
    
    # read file
    try:
        with open(path, 'r') as sourcefile:
            filecontent=sourcefile.read()

        # return file
        log(f'COMPLETED SUCCESSFULLY',"INFO")
        return json.loads(filecontent)
    except:
        log(f'{sys.exc_info()[0]:}',"ERROR")
        log(f'FAILED',"INFO")
        return

def log(message, type="INFO"):
    '''

    args:


    returns:
        
    
    '''
    frame = inspect.stack()[1]
    module = inspect.getmodule(frame[0])
    filename = module.__file__
    if type == 'IMPORTANT': 
        log_prefix = '********************************************************************************\n'
        log_sufix = '\n********************************************************************************'
        type = 'INFO'
    else:
        log_prefix = ''
        log_sufix = ''

    log_message = f'{log_prefix}{datetime.now():%Y-%m-%d %H:%M:%S} {type}: ({os.path.basename(filename)} - {frame[3]}) {message}{log_sufix}'

    if (LOGGING_MODE=='DEBUG' and type in ['INFO','WARNING','ERROR']) or (LOGGING_MODE=='WARNING' and type in ['WARNING','ERROR']) or LOGGING_MODE == type:
        if type == 'ERROR' or LOGGING_VERBOSE: print(log_message)
        if not os.path.exists('./logs/'): os.makedirs('./logs/')
        f = open(f'./logs/{os.path.basename(filename)}.log','a')
        f.write(f'{log_message}\n')

    return

def parse_args(args):
    '''

    args:


    returns:
        dictionary object
    
    '''
    arg_dict = {}
    arg_name = None
    position = -1
    
    for arg in args:
        ls = arg.split(',')
        if arg_name == None: position += 1
        if ls[0].lstrip().rstrip().startswith('-'):
            arg_name = ls[0]
            arg_dict[arg_name] = None
        else:
            if arg_name:
                arg_dict[arg_name] = ls
                arg_name = None
            else:
                arg_dict[position] = ls

    return arg_dict

if __name__=="__main__":
    args = parse_args(sys.argv)
    LOGGING_VERBOSE = False if not '--verbose' in args.keys() else True
    LOGGING_MODE = 'DEBUG' if not '--log' in args.keys() else args['--log']
    main(args)