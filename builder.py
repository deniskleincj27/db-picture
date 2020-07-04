#!/usr/bin/env python3

import os
import sys
import getpass
import imglib
import db_interface
import loc_interface
import util
import random
import time
from multiprocessing import Lock, Value, Queue, Process, current_process, Manager
import pprint
import collections

def build_user_db(data, q, max_download):

  num, user, dpath, dname, sql, reset, cursor, connection, loc, counter, lock = data

  p_pict = imglib.ProcessPicture(data)

  # print('build: ', num, ' done: ', done, ' offset_count: ', offset_count)
  if p_pict.m_reset:
    count = p_pict.reset()
  else:
    count, dirpath = p_pict.get_load_range(num)
    count = p_pict.load_range(num, count, dirpath)

  if num == 0:
    q.put(count)
  else:
    q.put((num, dirpath, count))

def do_it(user, dpath, dname):

  p_list = []

  reset = True
  num = 0
  cursor = None
  connection = None
  max_download = None

  sql = db_interface.SqlLite(dpath + dname)
  loc = loc_interface.GeoPy('user_agent = "zoby"')
  q = util.Queue()
  counter = util.Counter(0)
  lock = Lock()

  while True:

    data = (num, user, dpath, dname, sql, reset, cursor, connection, loc, counter, lock)
    p = util.Process(target = build_user_db, args = (data, q, max_download))
    p.start()

    if num == 0:
      p_num = q.get()
      cursor, connection = sql.open_db()
    else:
      p_list.append(p)

    reset = False
    if num == p_num or num > 20000:
      print(num, ' == ', p_num)
      break
    num += 1

  for p in p_list:
    p.join()
    # print('process done: ', p.name)

  print('\ndo_it qsize: ', q.qsize())
  items = [q.get() for _ in range(q.qsize())]
  tot = 0
  for item in items:
    num, dirpath, file_cnt = item
    tot += file_cnt

  print('tot files: ', tot)

  sql.close_db()


##################################################################################################################################################
#
#
##################################################################################################################################################

def worker(pnum, input, output, qdic):

  for fname, args in iter(input.get, 'STP'):
    output.put((pnum, args))
    data = qdic.get(pnum)
    if data is None:
      qdic.update({pnum : (1, [fname])})
    else:
      count, flist = data
      count += 1
      flist.append(fname)
      d = (count, flist)
      qdic[pnum] = d

# Producer function that places data on the Queue
def producer(queue, lock, names):

  for name in names:
    queue.put(name)

  # Synchronize access to the console
  with lock:
    print('Producer {0} exiting...'.format(queue.qsize()))
    pass

# The consumer function takes data off of the Queue
def consumer(queue, lock, done_queue):

  loc_iface = loc_interface.GeoPy('user_agent = "my_agent"')
  geolocator = loc_iface.get_geopy()

  # Run indefinitely
  while True:
    img_no, name = queue.get()

    with lock:
      rz_name, th_name, width, height = loc_iface.resize_picture(name, geolocator)
      done_queue.put((img_no, rz_name, th_name, width, height))


def produce_img(user, dpath, dname):

  sql = db_interface.SqlLite(dpath + dname)
  cursor, connection = sql.open_db()
  # Create the Queue object
  queue = Queue()
  done_queue = Queue()
  # Create a lock object to synchronize resource access
  lock = Lock()
  max_file = 400
  max_run = 40
  run_no = 0
  max_process = 10

  while True:

    mem, mval = util.check_memory()
    if mval < 1000000:
      print('mem: ', mem, ' run_no: ', run_no)
      break

    sql_stmt = 'select ImageNo, UrlOrg from PICTURE where UrlResize = \'''\'' #  Limit 10'
    cursor.execute(sql_stmt)
    row = cursor.fetchall()

    if len(row) == 0 or run_no == max_run:
      print ('stop at: ', len(row), 'mem: ', mem, ' run_no: ', run_no)
      break
    run_no += 1

    producers = []
    consumers = []

    nb_process = 0
    f_list = []

    if max_file > len(row):
      max_file = len(row)

    for idx, tup in enumerate(row):
      img_no, fname = tup
      filename = '/home/denis/Pictures/' + fname
      f_list.append((img_no, filename))
      if len(f_list) == max_file:
        producers.append(Process(target = producer, args = (queue, lock, f_list)))
        f_list = []
        nb_process += 1
      if nb_process > max_process:
        break

    print(' run_no: ', run_no, 'row: ', len(row), ' nb_process: ', nb_process, ' mem: ', mem)
    # create consumer processes
    for i in range(nb_process):
      p = Process(target = consumer, args = (queue, lock, done_queue))
      p.daemon = True
      consumers.append(p)

    for p in producers:
      p.start()
    for c in consumers:
      c.start()

    for p in producers:
      p.join()

    while True:
      if queue.qsize() == 0:
        break
      time.sleep(10)

    if done_queue.qsize() != 0:
      while True:
        img_no, rz_name, th_name, width, height = done_queue.get()
        sql_stmt = 'update PICTURE set UrlResize = \'' + rz_name + '\'' + ', UrlThumb = \'' + th_name + '\'  where ImageNo = ' + str(img_no)
        try:
          cursor.execute(sql_stmt)
        except Exception as e:
         print(sql_stmt, e)

        if done_queue.qsize() == 0:
          break

    connection.commit()

  sql.close_db()


#####################################################################################
#                                   main
#####################################################################################

if __name__ == "__main__":

  start = util.print_start()

  dpath = os.getcwd() + '/'
  dname = 'rad-picture.db'
  user = getpass.getuser()

#  print(util.__file__)
  s_mem, s_val = util.check_memory()
  print('version_info: ', sys.version_info.major, ' user: ', user, ' dpath: ', dpath)
  print('start {} mem: {}'.format(start, s_mem))

  if False:
    do_it(user, dpath, dname)
  else:
    produce_img(user, dpath, dname)

  e_mem, e_val = util.check_memory()
  print('end mem: {} used: {}'.format(e_mem, s_val - e_val))

  txt, start_time = util.print_elapse('', start)
  print('\n\n{0}, done...\n\n'.format(txt))


