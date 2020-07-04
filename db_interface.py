import abc
import sqlite3

class DataBase(abc.ABC):
  @abc.abstractmethod
  def open_db(self, name):
    print('DataBase abstractmethod!...')
    exit(0)

class MongoDb(DataBase):
  def open_db(self):
    # TBD...
    pass

class SqlLite(DataBase):

  def __init__(self, db_name):
    self.m_db_name = db_name

  def open_db(self):

    self.m_connection = sqlite3.connect(self.m_db_name)
    self.m_connection.text_factory = str  # allows utf-8 data to be stored
    self.m_cursor = self.m_connection.cursor()
    return self.m_cursor, self.m_connection

  def close_db(self):
    self.m_cursor.close()
    self.m_connection.close()

  def execute(self, sql_stmt):
    try:
      self.m_cursor.execute(sql_stmt)
    except Exception as e:
      print('--> execute: ', e, sql_stmt)

  def execute_commit(self, sql_stmt):
    try:
      self.execute(sql_stmt)
      self.commit()
    except Exception as e:
      print('execute_commit: ', e, sql_stmt)

  def commit(self):
    try:
      self.m_connection.commit()
    except Exception as e:
      print('commit: ', e)

  def save_current(self):
    try:
      self.m_connection.commit()
      self.close_db()
    except Exception as e:
      print('save_current: ', e)
      exit(0)

    return self.open_db()
