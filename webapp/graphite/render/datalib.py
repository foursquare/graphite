"""Copyright 2008 Orbitz WorldWide

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License."""

import array
import re
import socket
import struct
import time
import datetime
from collections import defaultdict
from django.conf import settings
from graphite.logger import log
from graphite.storage import STORE, LOCAL_STORE
from graphite.hypertable_client import HyperTablePool, removePrefix, addPrefix
from graphite.metrics.hypertable_search import hypertable_index
from hyperthrift.gen.ttypes import ScanSpec, CellInterval
from graphite.render.hashing import ConsistentHashRing

try:
  import cPickle as pickle
except ImportError:
  import pickle


class TimeSeries(list):
  def __init__(self, name, start, end, step, values, consolidate='average'):
    self.name = name
    self.start = start
    self.end = end
    self.step = step
    list.__init__(self,values)
    self.consolidationFunc = consolidate
    self.valuesPerPoint = 1
    self.options = {}

  def __iter__(self):
    if self.valuesPerPoint > 1:
      return self.__consolidatingGenerator( list.__iter__(self) )
    else:
      return list.__iter__(self)


  def consolidate(self, valuesPerPoint):
    self.valuesPerPoint = int(valuesPerPoint)


  def __consolidatingGenerator(self, gen):
    buf = []
    for x in gen:
      buf.append(x)
      if len(buf) == self.valuesPerPoint:
        while None in buf: buf.remove(None)
        if buf:
          yield self.__consolidate(buf)
          buf = []
        else:
          yield None
    while None in buf: buf.remove(None)
    if buf: yield self.__consolidate(buf)
    else: yield None
    raise StopIteration


  def __consolidate(self, values):
    usable = [v for v in values if v is not None]
    if not usable: return None
    if self.consolidationFunc == 'sum':
      return sum(usable)
    if self.consolidationFunc == 'average':
      return float(sum(usable)) / len(usable)
    raise Exception, "Invalid consolidation function!"


  def __repr__(self):
    return 'TimeSeries(name=%s, start=%s, end=%s, step=%s)' % (self.name, self.start, self.end, self.step)


  def getInfo(self):
    """Pickle-friendly representation of the series"""
    return {
      'name' : self.name,
      'start' : self.start,
      'end' : self.end,
      'step' : self.step,
      'values' : list(self),
    }



class CarbonLinkPool:
  def __init__(self, hosts, timeout):
    self.hosts = [ (server, instance) for (server, port, instance) in hosts ]
    self.ports = dict( ((server, instance), port) for (server, port, instance) in hosts )
    self.timeout = float(timeout)
    self.hash_ring = ConsistentHashRing(self.hosts)
    self.connections = {}
    self.last_failure = {}
    # Create a connection pool for each host
    for host in self.hosts:
      self.connections[host] = set()

  def select_host(self, metric):
    "Returns the carbon host that has data for the given metric"
    return self.hash_ring.get_node(metric)

  def get_connection(self, host):
    # First try to take one out of the pool for this host
    (server, instance) = host
    port = self.ports[host]
    connectionPool = self.connections[host]
    try:
      return connectionPool.pop()
    except KeyError:
      pass #nothing left in the pool, gotta make a new connection

    log.cache("CarbonLink creating a new socket for %s" % str(host))
    connection = socket.socket()
    connection.settimeout(self.timeout)
    try:
      connection.connect( (server, port) )
    except:
      self.last_failure[host] = time.time()
      raise
    else:
      connection.setsockopt( socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1 )
      return connection

  def query(self, metric):
    request = dict(type='cache-query', metric=metric)
    results = self.send_request(request)
    log.cache("CarbonLink cache-query request for %s returned %d datapoints" % (metric, len(results)))
    return results['datapoints']

  def get_metadata(self, metric, key):
    request = dict(type='get-metadata', metric=metric, key=key)
    results = self.send_request(request)
    log.cache("CarbonLink get-metadata request received for %s:%s" % (metric, key))
    return results['value']

  def set_metadata(self, metric, key, value):
    request = dict(type='set-metadata', metric=metric, key=key, value=value)
    results = self.send_request(request)
    log.cache("CarbonLink set-metadata request received for %s:%s" % (metric, key))
    return results

  def send_request(self, request):
    metric = request['metric']
    serialized_request = pickle.dumps(request, protocol=-1)
    len_prefix = struct.pack("!L", len(serialized_request))
    request_packet = len_prefix + serialized_request

    host = self.select_host(metric)
    conn = self.get_connection(host)
    try:
      conn.sendall(request_packet)
      result = self.recv_response(conn)
    except:
      self.last_failure[host] = time.time()
      raise
    else:
      self.connections[host].add(conn)
      if 'error' in result:
        raise CarbonLinkRequestError(result['error'])
      else:
        return result

  def recv_response(self, conn):
    len_prefix = recv_exactly(conn, 4)
    body_size = struct.unpack("!L", len_prefix)[0]
    body = recv_exactly(conn, body_size)
    return pickle.loads(body)


# Utilities
class CarbonLinkRequestError(Exception):
  pass

def recv_exactly(conn, num_bytes):
  buf = ''
  while len(buf) < num_bytes:
    data = conn.recv( num_bytes - len(buf) )
    if not data:
      raise Exception("Connection lost")
    buf += data

  return buf

#parse hosts from local_settings.py
hosts = []
for host in settings.CARBONLINK_HOSTS:
  parts = host.split(':')
  server = parts[0]
  port = int( parts[1] )
  if len(parts) > 2:
    instance = parts[2]
  else:
    instance = None

  hosts.append( (server, int(port), instance) )


#A shared importable singleton
CarbonLink = CarbonLinkPool(hosts, settings.CARBONLINK_TIMEOUT)

# Data retrieval API
def fetchData(requestContext, pathExpr):
  return fetchDataFromHyperTable(requestContext, pathExpr)

def fetchDataFromHyperTable(requestContext, pathExpr):
  #TODO: use django settings
  MIN_INTERVAL_SECS = 10
  COL_INTERVAL_SECS = 60 * 60


  log.info('fetching %s' % pathExpr)
  pathExpr = addPrefix(pathExpr)
  metricData = hypertable_index.findMetric(pathExpr)
  metrics = [addPrefix(m[0]) for m in metricData]
  metricRate = {}
  for m in metricData:
    if not m[1]:
      log.info("metric %s doesn't specify a rate! Not rendering..." % m[0])
      metrics.remove(m[0])
    else:
      metricRate[m[0]] = m[1]

  if not metrics:
    return []

  startDateTime = requestContext['startTime']
  endDateTime = requestContext['endTime']

  start, end = int(timestamp(requestContext['startTime'])), int(timestamp(requestContext['endTime']))

  startColString = startDateTime.strftime('metric:%Y-%m-%d %H')
  endColString = endDateTime.strftime('metric:%Y-%m-%d %H')
  cellIntervals = [ CellInterval(m, startColString, True, m, endColString, True) for m in metrics ]

  if cellIntervals == None:
    return []

  nanosStart = start * 10**9L
  nanosEnd = end * 10**9L

  scan_spec = ScanSpec(None, None, None, 1)
  scan_spec.start_time = nanosStart
  scan_spec.end_time = nanosEnd
  scan_spec.cell_intervals = cellIntervals
  scan_spec.versions = COL_INTERVAL_SECS / MIN_INTERVAL_SECS

  log.info(startDateTime)
  log.info(endDateTime)
  log.info(scan_spec)

  valuesMap = defaultdict(list)

  def processResult(key, family, column, val, ts):
    its = long(ts) / 10**9L  #nanoseconds -> seconds
    valuesMap[key].append((its, val))



  HyperTablePool.doScan(scan_spec, "metrics", processResult)

  elapsed = end - start

  for m in valuesMap.keys():
    # resample everything to 'best' granularity
    steps = int(end - start) / metricRate[m]

    # push final values
    finalValues = [None] * steps
    for x in valuesMap[m]:
      bucket = int(min(round(float(x[0] - start) / float(metricRate[m])), steps - 1))
      finalValues[bucket] = float(x[1])
    valuesMap[m] = finalValues

  seriesList = []
  for m in sorted(valuesMap.keys()):
    series = TimeSeries(removePrefix(m), start, end, metricRate[m], valuesMap[m])
    series.pathExpression = pathExpr # hack to pass expressions through to render functions
    seriesList.append(series)

  return seriesList

def fetchDataLocal(requestContext, pathExpr):
  seriesList = []
  startTime = requestContext['startTime']
  endTime = requestContext['endTime']

  if requestContext['localOnly']:
    store = LOCAL_STORE
  else:
    store = STORE

  for dbFile in store.find(pathExpr):
    log.metric_access(dbFile.metric_path)
    dbResults = dbFile.fetch( timestamp(startTime), timestamp(endTime) )
    try:
      cachedResults = CarbonLink.query(dbFile.real_metric)
      results = mergeResults(dbResults, cachedResults)
    except:
      log.exception()
      results = dbResults

    if not results:
      continue

    (timeInfo,values) = results
    (start,end,step) = timeInfo
    series = TimeSeries(dbFile.metric_path, start, end, step, values)
    series.pathExpression = pathExpr #hack to pass expressions through to render functions
    seriesList.append(series)

  return seriesList


def mergeResults(dbResults, cacheResults):
  cacheResults = list(cacheResults)

  if not dbResults:
    return cacheResults
  elif not cacheResults:
    return dbResults

  (timeInfo,values) = dbResults
  (start,end,step) = timeInfo

  for (timestamp, value) in cacheResults:
    interval = timestamp - (timestamp % step)

    try:
      i = int(interval - start) / step
      values[i] = value
    except:
      pass

  return (timeInfo,values)


def timestamp(datetime):
  "Convert a datetime object into epoch time"
  return time.mktime( datetime.timetuple() )

