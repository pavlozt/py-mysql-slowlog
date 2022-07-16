""" Module for reading MySQL slow log files as Pandas dataframe. Like pt-query-digest for Statisticians. """

import re
import pandas as pd
from datetime import datetime
import gzip


read_chunk_size = 40000

def myreadlines(f, newline):
  buf = ""
  while True:
    while newline in buf:
      pos = buf.index(newline)
      yield buf[:pos]
      buf = buf[pos + len(newline):]
    chunk = f.read(read_chunk_size)
    if not chunk:
      yield buf
      break
    buf += chunk

# set of precompiled regex
re_initial_timestamp = re.compile(r"^(\d\d)(\d\d)(\d\d)\s+(\d{1,2}):(\d\d):(\d\d)\n")

re_split_by_comments = "(.+?#"
re_split_stats_and_sql = re.compile(r"^(?:# ([^\n]+))",re.M)
timedelimiter = '# Time: '

re_usedb_cutter = re.compile("\nuse (.+?);\n",  re.IGNORECASE | re.M| re.S)
re_setts_cutter = re.compile("\nSET timestamp=\d+;\n" ,re.IGNORECASE | re.M| re.S)

tech_exclude_properties = { 'user@host' : True , 'thread_id' : True}

column_names_ts = ['timestamp']
column_names_str = ['schema','sqltext']
column_names_int =  ['rows_sent','rows_examined','tmp_table_sizes',]
column_names_float =  ['query_time','lock_time',]
column_names_bool =  ['tmp_table_on_disk','full_scan']

column_names = column_names_ts + column_names_str + column_names_int + column_names_float + column_names_bool

def read( filename:str ,  save_sql: bool = True):
  """ Read mysql log file as pandas dataframe.
  :param filename: file name
  :param timestamp_index: convert timestamp as dataframe index. This is comfortable behavior.
  :param save_sql: save or noy  query text when parsing. Removing will save memory.
  :return: pandas.DataFrame

  """
  # It is more efficently create pandas dataframe from list of named lists.
  data = {x: list() for x in column_names}

  if filename.endswith('.gz'):
    f = gzip.open(filename,'rt',encoding='utf-8')
  else:
    f = open(filename,'rt',encoding='utf-8')
  with f:
    for line in myreadlines(f, ";\n" + timedelimiter):
      ts_match = re_initial_timestamp.search(line)
      if  ts_match :
          # конструируем время
          ts = datetime(2000+int(ts_match[1]), int(ts_match[2]), int(ts_match[3]),
              int(ts_match[4]), int(ts_match[5]), int(ts_match[6]))
          block = line[ts_match.end():]
          # обработаем в цикле совпадение с регуляркой блоков строк
          lastpos = 0
          record = {}
          for m in re_split_stats_and_sql.finditer(block):
            for kv in m[1].split('  '):
              (k,v) =kv.split(': ')
              k=k.lower()
              if not k in tech_exclude_properties:
                record[k]=v
              lastpos = m.end()
          record['timestamp'] = ts.strftime('%Y/%m/%d %H:%M:%S')
          #print('parsed', record)
          if save_sql:
            sqlblock=block[lastpos:]

            sqlblock = re_usedb_cutter.sub('',sqlblock)
            sqlblock = re_setts_cutter.sub('',sqlblock)
            record['sqltext']=sqlblock
          else :
            record['sqltext']= ''

          for name in column_names :
            if name in record:
              data[name].append(record[name])
            else:
              data[name].append(None)

      else:
        pass ; # skip not parsed. some records without timestamp. this is  restart server records.


  df = pd.DataFrame.from_dict(data)
  # now df is list of string. Convert to appropriate numpy types.
  for n in column_names_bool:
    df[n] = df[n].astype('bool')
  for n in column_names_float:
    df[n]= pd.to_numeric(df[n],errors='coerce').astype('float')

  for n in column_names_int:
    df[n] = df[n].fillna(0).astype('int64')

  for n in column_names_ts:
    df[n]=pd.to_datetime(df[n])


  return df


