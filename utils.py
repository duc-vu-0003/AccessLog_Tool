import re
from os import path

class Paths(object):
  data = 'data'
  temp_data = ".temp"
  result = 'result'
  major = 'major'
  test_path = path.join(data, 'Test1.csv')
  arff_path = path.join(data, 'Test1.arff')
  cache_file = path.join(temp_data, 'cache.npz')

  ipList_file = path.join(temp_data, 'ipList.npz')
  timeList_file = path.join(temp_data, 'timeList.npz')
  methodList_file = path.join(temp_data, 'methodList.npz')
  requestLinkList_file = path.join(temp_data, 'requestLinkList.npz')
  statusCodeList_file = path.join(temp_data, 'statusCodeList.npz')
  requestLineList_file = path.join(temp_data, 'requestLineList.npz')
  refererLinkList_file = path.join(temp_data, 'refererLinkList.npz')
  userAgentList_file = path.join(temp_data, 'userAgentList.npz')
