import sys
import time
import subprocess
import os.path
from django.conf import settings
from hyperthrift.gen.ttypes import ScanSpec
from graphite.hypertable_client import removePrefix, addPrefix
from graphite.storage import _deduplicate, is_pattern
from graphite.logger import log
import re
import fnmatch

from graphite.hypertable_client import HyperTablePool

EXPANDABLE_PATH_RE = re.compile('.*[\*{}\[\]]+.*')
def regexifyPathExpr(pathExpr):
  return pathExpr.replace('+', '\\+').replace('.', '\\.').replace('*', '[^\.]+')

CACHE_CHECK_INTERVAL_SECS = 300

class HyperIndex:
  def __init__(self):
    self.index_path = 'index.txt'
    self.last_atime = 0
    self.every_metric = ''
    self.tree = ({}, {})
    log.info("[HyperIndex] performing initial index load")
    self._loadFromFile()
    self._loadFromHyperTable()

  def _loadFromFile(self):
    if os.path.exists(self.index_path):
      s = time.time()
      self.last_atime = int(os.path.getmtime(self.index_path)) * 10**9L
      fh = open(self.index_path)
      for l in fh:
        self._add(l.strip())
      fh.close()
      log.info("[HyperIndex] initial load took %.6f seconds" % (time.time() - s))


  def _loadFromHyperTable(self):
    # if the index_path is suddenly deleted, that means start from scratch
    if not os.path.exists(self.index_path):
      self.last_atime = 0
    spec = ScanSpec(keys_only=True, start_time=self.last_atime, versions=1)
    s = time.time()
    self.last_atime = int(s) * 10**9L
    metrics = []
    fh = open(self.index_path, 'a')
    def processResult(key, family, column, val, ts):
      if not self._existsInTree(key):
        fh.write(key + '\n')
        self._add(key)

    # TODO(johng): convert to a doScan once SCR works on large results
    HyperTablePool.doScanAsArrays(spec, "search", processResult)
    fh.close()
    log.info("[HyperIndex] index reload took %.6f seconds" % (time.time() - s))

  # like find in tree, for exact matches.
  # for index dup checking
  def _existsInTree(self, key):
    branches = key.split('.')
    cursor = self.tree
    leaf = branches.pop()
    for branch in branches:
      if branch not in cursor[1]:
        return False
    if leaf in cursor[0]:
      return True
    else:
      return False

  def _add(self, key):
    branches = key.split('.')
    cursor = self.tree
    leaf = branches.pop()
    for branch in branches:
      if branch not in cursor[1]:
        cursor[1][branch] = ({}, {}) # (leaves, children)
      cursor = cursor[1][branch]
    cursor[0][leaf] = 1 # add leaf

  def _getMatches(self, haystack_dict, needle):
    if type(needle) is list: # patterns, variants
      entries = haystack_dict.keys()
      matches = []
      for variant in needle:
        matches.extend(fnmatch.filter(entries, variant))
      return list(_deduplicate(matches))
    else:
      if needle in haystack_dict:
        return [needle]
      else:
        return[]

  # splits the key by '.', exact parts are strings, patterns are lists
  def _split(self, key):
    parts = key.split('.')
    for i in range (0, len(parts)):
      if is_pattern(parts[i]):
        parts[i] = self._variants(parts[i])
    return parts

  # computes variants in a pathExpr
  def _variants(self, pattern):
    v1, v2 = pattern.find('{'), pattern.find('}')
    if v1 > -1 and v2 > v1:
      variations = pattern[v1+1:v2].split(',')
      variants = [ pattern[:v1] + v + pattern[v2+1:] for v in variations ]
      return variants
    return [pattern]


  def _findInTree(self, cursor, keyparts, leaf_matches_leaves=True, leaf_matches_branches=False):
    if not keyparts:
      return []
    #print keyparts
    part = keyparts.pop()
    if len(keyparts) == 0: #leaf
      res = []
      if leaf_matches_leaves:
        res.extend([([e], True) for e in self._getMatches(cursor[0], part)])
      if leaf_matches_branches:
        res.extend([([e], False) for e in self._getMatches(cursor[1], part)])
      #print res
      return res
    else:
      results = []
      for match in self._getMatches(cursor[1], part):
        #print match
        postfixes = self._findInTree(cursor[1][match], keyparts[:], leaf_matches_leaves, leaf_matches_branches)
        for postfix in postfixes:
          postfix[0].append(match)
          results.append(postfix)
      return results 

  def findInTree(self, pathExpr, leaf_matches_leaves=True, leaf_matches_branches=False):
    if int(time.time()) * 10**9L - self.last_atime > CACHE_CHECK_INTERVAL_SECS * 10**9L:
      self._loadFromHyperTable()
    s = time.time()
    parts = self._split(pathExpr)
    parts.reverse() #keyparts is reversed, because pop is fast
    res = self._findInTree(self.tree, parts, leaf_matches_leaves, leaf_matches_branches)
    nodes = [HyperNode('.'.join(reversed(x[0])), x[1]) for x in res]
    
    log.info("[HyperIndex] search for %s took %.6f seconds" % (pathExpr, time.time() - s))
    return nodes

  # only returns metrics
  def findMetric(self, pathExpr):
    return [node.metric_path for node in self.findInTree(pathExpr)]

  # returns HyperNodes which could be metrics, or subfolders
  def find(self, pathExpr):
    return self.findInTree(pathExpr, True, True)

  # weird format for seemingly deprecated metrics/search endpoint
  def search(self, query, max_results=None, keep_query_pattern=False):
    count = 0
    for node in self.find(query):
      if max_results != None and count >= max_results:
        return
      yield { 'path': node.metric_path, 'is_leaf': node.isLeaf() }
      count += 1

class HyperStore:
  def find(self, pathExpr):
    pathExpr = pathExpr
    log.info('searching for: %s' % pathExpr)
    if EXPANDABLE_PATH_RE.match(pathExpr):
      regex = regexifyPathExpr(pathExpr)
      where = 'ROW REGEXP "%s"' % regex

      starIndex = pathExpr.find('*')
      if starIndex > 0:
        where += ' AND ROW =^ "%s"' % pathExpr[0:starIndex]

      log.info('where: %s' % where)
      return [removePrefix(p) for p in self.findHelper(where)]
    else:
      return [removePrefix(pathExpr)]

  def search(self, query):
    qre = '(?i)%s' % re.sub('\*', '.*', re.sub('\.', '\.', query))
    return [removePrefix(p) for p in self.findByRegex(qre)]


  def findByRegex(self, regex):
    where = 'ROW REGEXP "%s"' % regex
    return self.findHelper(where)

  def findHelper(self, where):
    query = 'SELECT * FROM search WHERE %s' % (where)

    metrics = []
    def processResult(key, family, column, val, ts):
      metrics.append(key)

    HyperTablePool.doQueryAsArrays(query, processResult)
    return metrics

class HyperNode:
  context = {}

  def __init__(self, metric_path, isLeaf):
    self.metric_path = metric_path
    self.real_metric = metric_path
    self.name = metric_path.split('.')[-1]
    self.__isLeaf = isLeaf

  def isLeaf(self):
    return self.__isLeaf                                                                                                                                                                            
  def __repr__(self):
    return 'HyperNode(%s, %s)' % (self.metric_path, self.isLeaf())


hypertable_index = HyperIndex()
