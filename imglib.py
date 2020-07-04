#!/usr/bin/env python3

import os
import time
import db_interface
import json
import util
import threading
import random
from multiprocessing import Lock, Queue

dic_state = ['not-done', 'in-progress', 'complete']

##########################################################################################################
#
#
##########################################################################################################

class ProcessPicture(object):

  def __init__(self, data, max_download = None):

    pnum, user, dpath, db_name, sql, reset, cursor, connection, loc, counter, lock = data

    self.m_fno = 0
    self.m_dir_dic = {}
    self.m_cursor = cursor
    self.m_connection = connection
    self.m_pnum = pnum
    self.m_pic_path = '/home/' + user + '/Pictures'
    self.m_dest_path = dpath
    self.m_db_name = db_name
    self.m_db_interface = sql
    self.m_loc_interface = loc
    self.m_lock = lock
    self.m_reset = reset
    self.m_counter = counter
    self.m_max_download = max_download
    self.m_s_time = util.get_time_now()
    self.m_queue = []

  def format_date(self, date):

    if date is 'Unknown':
      return date
    pos = date.find(' ')
    assert pos != -1
    date = date.replace(':', '-', 2)
    return date

  def clean_input(self, strg):

    strg = strg.replace('\'', '_')
    strg = '\'' + strg + '\''

    return strg

  def clean_dir(self, ext):

    for dirpath, _, filenames in os.walk(self.m_pic_path):

      fpath = dirpath
      fpath = fpath.replace('.', '') + '/'
      for filename in filenames:
        if filename.lower().find(ext.lower()) != -1:
          try:
            os.remove(fpath + filename)
          except Exception as e:
            print('error: {0}'.format(e))

  def reset(self):

    self.clean_dir('-resize.JPG')
    self.clean_dir('-thumb.JPG')
    self.build_json_dic()
    self.save_json_dic()

    try:
      os.remove(self.m_db_name)
    except Exception as e:
      print('self.m_db_name e: ', e)
      pass

    self.m_cursor, self.m_connection = self.m_db_interface.open_db()
    self.create_table('PICTURE')
    print('reset done len: {0} {1} {2}'.format(len(self.m_dir_dic), self.m_pic_path, self.m_db_name))
    self.m_db_interface.close_db()

    return len(self.m_dir_dic)

  def create_table(self, table):

    sql_stmt = "DROP TABLE IF EXISTS %s" % table
    self.m_db_interface.execute(sql_stmt)

    sql_stmt = "CREATE TABLE %s (ImageNo integer PRIMARY KEY, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" \
      % (table, 'FileName',  'UrlOrg', 'UrlResize', 'UrlThumb', 'Date', 'Orientation', 'Location', 'Width', 'Height', 'Make', 'Model', 'View')
    self.m_db_interface.execute_commit(sql_stmt)

  def read_json(self):

    if os.path.isfile(self.m_dest_path + 'json_dir.json'):
      with open(self.m_dest_path + 'json_dir.json') as json_file:
        self.m_dir_dic = json.load(json_file)

  def save_json_dic(self):

    jfile = json.dumps(self.m_dir_dic)
    f = open('./json_dir.json', 'w')
    f.write(jfile)
    f.close()

  def build_json_dic(self):

    exclude_dir = []
    self.m_dir_dic = {}

    # print('build_json_dic: ', self.m_pic_path)
    for dirpath, _, filenames in os.walk(self.m_pic_path):
      if any(ext in dirpath for ext in exclude_dir):
        continue

      fpath = dirpath
      fpath = fpath.replace('.', '') + '/'
      dirpath += '/'
      file_no = 0
      for filename in filenames:
        _, ext = os.path.splitext(fpath + filename)
        if ext.lower() == '.jpg' or ext.lower() == '.jpeg':
          file_no += 1

      if file_no != 0:
        # print('fpath {0} {1}'.format(file_no, fpath))
        assert self.m_dir_dic.get(fpath) == None
        self.m_dir_dic.update({fpath : (file_no, dic_state[0])})

    print('build_json_dic len: {0} {1}'.format(len(self.m_dir_dic), self.m_pic_path))

  def get_load_range(self, num):

    offset_count = 0
    count = -1
    assert num == self.m_pnum

    with self.m_lock:
      self.read_json()

      for dirpath, tup in sorted(self.m_dir_dic.items()):

        dcount, flag = tup
        if flag == dic_state[2]:
          offset_count += dcount
          continue
        if flag == dic_state[1]:
          offset_count += dcount
          continue

        assert flag == dic_state[0]
        self.m_dir_dic[dirpath] = [dcount, dic_state[1]]
        count = dcount
        self.save_json_dic()
        break

    return count, dirpath

  def load_range(self, num, count, dict_path): # file_sno, path, dir_dic, cursor, connection):

    with self.m_lock:

      _, state = self.m_dir_dic[dict_path]
      assert state == dic_state[1]
      assert num == self.m_pnum

    self.spawn_thread(count, dict_path)

    return len(self.m_queue)

  def spawn_thread(self, offset_count, dict_path):

    dcount, _ = self.m_dir_dic[dict_path]
    max_download = 20
    max_len = dcount
    thread_list = []
    th_lock = Lock()

    if max_download > max_len:
      max_download = max_len

    count = 0
    idx   = 0

    while count < max_len:

      start_index = idx * max_download
      end_index   = start_index + max_download

      if end_index >= max_len:
        end_index = max_len

      thread = util.threading.Thread(target = self.get_thread_range, args = (dict_path, start_index, end_index, self.m_lock))
      thread.start()
      thread_list.append((thread, end_index - start_index))

      count += max_download
      idx += 1

    for thread, count in thread_list:
      thread.join()

    with self.m_lock:

      for sql_stmt in self.m_queue:
        self.m_db_interface.execute(sql_stmt)

      try:
        self.m_db_interface.commit()
      except Exception as e:
        print('spawn_thread Exception: ',  e, self.m_pnum, dict_path)

    return dcount

  def get_thread_range(self, dict_path, s_idx, e_idx, th_lock):

    date, location, orientation, name, filename, rz_name, th_name = self.dummy_return()

    geolocator = self.m_loc_interface.get_geopy()
    filenames = os.listdir(dict_path)

    for idx in range(s_idx, e_idx):
      name = filenames[idx]
      filename = dict_path + name
      _, ext = os.path.splitext(filename)
      if not (ext.lower() == '.jpg' or ext.lower() == '.jpeg'):
        # print(' process: {0} {1} {2}'.format(self.m_pnum, filename, ext))
        continue

      mem = util.check_memory()
      date, location, orientation, rz_name, th_name, width, height, make, model = self.m_loc_interface.get_picture_data(filename, geolocator)
      date = self.format_date(date)

      h_path = '/home/denis/Pictures/'
      pos = filename.find(h_path)
      assert pos != -1
      filename = filename[pos + len(h_path):]

      with self.m_lock:
        sql_stmt = 'INSERT INTO PICTURE (ImageNo, Date, Location, Orientation, FileName, UrlOrg, UrlResize, UrlThumb, Width, Height, Make, Model, View) \
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)' \
          % (self.m_counter.value(), self.clean_input(date), self.clean_input(str(location)), self.clean_input(str(orientation)),
          self.clean_input(name), self.clean_input(filename), self.clean_input(rz_name), self.clean_input(th_name), str(width), str(height),
          self.clean_input(make), self.clean_input(model), '0')
        self.m_counter.increment()

      self.m_queue.append(sql_stmt)

  def dummy_return(self):

    date = ''
    location = ''
    orientation = ''
    name = ''
    filename = ''
    rz_name = ''
    th_name = ''

    return date, location, orientation, name, filename, rz_name, th_name


