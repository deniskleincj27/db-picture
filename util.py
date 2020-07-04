from multiprocessing import Process, Lock, Queue, Manager, Value
from datetime import datetime as dt
import time
import threading
from collections import namedtuple

##########################################################################################################
#
#                             Util
#
##########################################################################################################

class Counter(object):

  def __init__(self, initval=0):
    self.val = Value('i', initval)
    self.lock = Lock()

  def increment(self):
    with self.lock:
      self.val.value += 1

  def value(self):
    with self.lock:
      return self.val.value

def get_time():
  return time.time()

def get_time_now():
  return dt.now()

def check_memory():

  MemInfoEntry = namedtuple('MemInfoEntry', ['value', 'unit'])
  meminfo = {}

  with open('/proc/meminfo') as file:
    for line in file:
      key, value, *unit = line.strip().split()
      meminfo[key.rstrip(':')] = MemInfoEntry(value, unit)

  tup = meminfo['MemAvailable']
  txt = '{:,}'.format(int(tup.value))

  return txt, int(tup.value)

def print_elapse(title, stime, prt = 0):

  etime = dt.now()
  elapse = etime - stime

  hrs = int(elapse.seconds / (60 * 60))
  mn = int((elapse.seconds - hrs * 60 * 60) / (60))
  sec = int(elapse.seconds - (hrs * 60 * 60) - (mn * 60))

  # print(title, ' done in %02d:%02d:%02d' %(hrs, mn, sec))
  if prt:
    txt = ' ...{:02}:{:02}:{:02}'.format(hrs, mn, sec)
  else:
    txt = ' done in ...{:02}:{:02}:{:02}'.format(hrs, mn, sec)

  return title + txt, etime

def prt_time(s_time):

  txt_time = str(s_time)
  pos = txt_time.find('.')

  if pos == -1:
    return txt_time

  return txt_time[:pos]

def print_start():

  ts_now = get_time()
  st_start = dt.fromtimestamp(ts_now).strftime('%Y-%m-%d %H:%M:%S')
  print('starting building at.... {0}\n'.format(st_start))

  return dt.now()

def print_end(st, prt = 0):

  txt, _ = print_elapse('', st, prt)
  if prt:
    return txt

  print('\n\n{0}, done...\n\n'.format(txt))
  return txt
