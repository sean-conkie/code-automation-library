
import json
import os
import re
import sys

from google.cloud import storage
from datetime import datetime
from os import environ, path
from trigger_dag import trigger_dag

# local_folder = path.dirname(path.abspath(__file__))

local_folder = '/tmp'
load_bucket = storage.Client().get_bucket('mfp-load')

def landing_function(event, context):
  """Background Cloud Function to be triggered by Cloud Storage.
      This generic function logs relevant data when a file is changed.
  Args:
      event (dict):  The dictionary with data specific to this type of event.
                      The `data` field contains a description of the event in
                      the Cloud Storage `object` format described here:
                      https://cloud.google.com/storage/docs/json_api/v1/objects#resource
      context (google.cloud.functions.Context): Metadata of triggering event.
  Returns:
      None; the output is written to Stackdriver Logging
  """

  # capture file name, file dt, load dt for insert into the file
  file_name = event['name']
  bucket_name = event['bucket']
  m = extract_file_dt(file_name)
  file_dt = m if m else event['updated']
  load_dt = event['updated']

  name, extension = os.path.splitext(event['name'])

  if extension == '.json':
    add_fields_json(bucket_name, file_name, file_dt, load_dt)

  if 'team' in file_name.split('_'):
    trigger_dag('team-load', event)
  



def extract_file_dt(name):
  '''Method to extract a file date from the file name.
      This generic method uses regex to search for date and time patterns.
  args:
      name (string):  The file name to be searched.
  returns:
      string of datetime'''
      
  m = re.search(r'(?P<year>2[0-9]{3})[-_.]?(?P<month>[0-1]{1}[0-9]{1}|JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)[-_.]?(?P<day>[0-3]{1}[0-9]{1})[-_.]?((?P<hour>[0-2]{1}[0-3]{1})[-_.]?(?P<minute>[0-5]{1}[0-9]{1}))?',name,re.IGNORECASE)

  if not m:
    m = re.search(r'(?P<day>[0-3]{1}[0-9]{1})[-_.]?(?P<month>[0-1]{1}[0-9]{1}|JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)[-_.]?(?P<year>2[0-9]{3})[-_.]?((?P<hour>[0-2]{1}[0-3]{1})[-_.]?(?P<minute>[0-5]{1}[0-9]{1}))?',name,re.IGNORECASE)

  if m.group('hour'):
    file_dt = str(datetime(int(m.group('year')),int(m.group('month')),int(m.group('day')),int(m.group('hour')),int(m.group('minute'))))
  else:
    file_dt = str(datetime(int(m.group('year')),int(m.group('month')),int(m.group('day'))))

  return file_dt


def add_fields_json(bucket_name, file_name, file_dt, load_dt):
  '''Method to add load fields to source file.
  
  args:
      bucket_name (string):

      file_name (string):
      
      file_dt (string):
      
      load_dt (string):
      
  returns:
      0 on success
      1 on error'''

  bucket = storage.Client().get_bucket(bucket_name)

  blob = bucket.blob(file_name)
  blob_name = blob.name.split('/')[-1]
  local_file = f'{local_folder}/{blob_name}'

  blob.download_to_filename(local_file)

  print(f'opening object {local_file}')
  with open(local_file,'r') as sourcefile:
      filecontent=sourcefile.read()

  j = json.loads(filecontent)

  if not 'file_name' in  j.keys():
    j['file_name'] = file_name
    
  if not 'file_dt' in  j.keys():
    j['file_dt'] = file_dt
    
  if not 'load_dt' in  j.keys():
    j['load_dt'] = load_dt

  outp = json.dumps(j, separators=(",", ":"))

  print(f'writing to object {local_file}')
  with open(local_file,'w') as outfile:
    outfile.write(outp)

  print(f'uploading {local_file} to {load_bucket}')
  blob = load_bucket.blob(file_name)
  blob.upload_from_filename(local_file)

  print(f'Delete original file')
  bucket.delete_blob(file_name)

  print(f'complete')

  return 0
