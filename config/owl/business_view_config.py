BUSINESS_METRICS_VIEW_CONFIG = {
  'Write HBase' : [['write operation', 'Write/HBase'],
                   ['log length', 'Write/Log'],
                   ['memory/thread', 'Write/MemoryThread'],
  ],
  'Read HBase' : [['read operation', 'Read/HBase'],
                   ['result size', 'Read/ResultSize'],
                   ['memory/thread', 'Read/MemoryThread'],
  ],
}

ONLINE_METRICS_MENU_CONFIG = {
  'Online Write' : [['Qps', 'Write/Qps'],
                    ['HBase Latency', 'Write/HBase Latency'],
                    ['Total Latency', 'Write/Total Latency'],
                    ['WriteFail', 'Write/WriteFail'],
                    ['HTablePool', 'Write/HTablePool'],
                    ['Replication', 'Write/Replication'],
                    ['Exception', 'Write/Exception'],
  ],
  'Online Read' : [['Qps', 'Read/Qps'],
                   ['HBase Latency', 'Read/HBase Latency'],
                   ['Total Latency', 'Read/Total Latency'],
                   ['ReadFail', 'Read/ReadFail'],
                   ['HTablePool', 'Read/HTablePool'],
                   ['Exception', 'Read/Exception'],
  ],
}

ONLINE_METRICS_COUNTER_CONFIG = {
}

ONLINE_METRICS_TITLE_CONFIG = {
}

ONLINE_METRICS_ENDPOINT_CONFIG = {
}
