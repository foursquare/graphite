from __future__ import with_statement

from django.conf import settings
from graphite.logger import log
import hypertable.thriftclient
import threading
import re
import time

def removePrefix(path):
  if settings.HYPERTABLE_PREFIX:
    log.info(path)
    log.info(settings.HYPERTABLE_PREFIX)
    return re.sub('^%s\.' % settings.HYPERTABLE_PREFIX, '', path)
  else:
    return path

def addPrefix(path):
  if settings.HYPERTABLE_PREFIX:
    return '%s.%s' % (settings.HYPERTABLE_PREFIX, path)
  else:
    return path

class ConnectionPool:
  def makeClient(self):
    (host, port) = settings.HYPERTABLE_SERVER.split(':')
    return hypertable.thriftclient.ThriftClient(host, int(port))
  
  def __init__(self, count):
    self.freeClients = [self.makeClient() for x in range(0, count)]
    self.semaphore = threading.BoundedSemaphore(count)
    self.lock = threading.Lock()

  def getConn(self):
    with self.lock:
      return self.freeClients.pop()

  def releaseConn(self, conn):
    with self.lock:
      return self.freeClients.append(conn)

  def doScan(self, spec, table, cb):
    with self.semaphore:
      start = time.time()
      conn = self.getConn()
      namespace = conn.namespace_open('monitor')
      scanner = conn.scanner_open(namespace, table, spec)

      while True:
        row_data = conn.scanner_get_cells_as_arrays(scanner)
        if(len(row_data) == 0):
          break
        for key, family, column, val, ts in row_data:
          cb(key, family, column, val, ts)

      conn.close_scanner(scanner)
      self.releaseConn(conn)
      log.info(spec)
      log.info('fetch time: %s' % (time.time() - start))



  def doQuery(self, query, cb):
    with self.semaphore:
      conn = self.getConn()
      namespace = conn.namespace_open('monitor')
      results =  conn.hql_exec2(namespace, query, 0, 1)

      while True:
        row_data = conn.next_row_as_arrays(results.scanner)
        if not row_data:
          break
        for key, family, column, val, ts in row_data:
          cb(key, family, column, val, ts)

      conn.close_scanner(results.scanner)
      self.releaseConn(conn)

HyperTablePool = ConnectionPool(20)
