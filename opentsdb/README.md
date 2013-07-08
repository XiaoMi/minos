# Opentsdb metrics collector
[Opentsdb](http://opentsdb.net) is used to store and display metrics of clusters.

# Installation
Setup opentsdb
<http://opentsdb.net/getting-started.html>

Configure for metrics collector

Modify file in config/owl/opentsdb/metrics_collector_config.py

    # metrics's output url in owl
    metrics_url = 'http://127.0.0.1:8000/monitor/metrics'
    # opentsdb's binary path
    opentsdb_bin_path = 'tsdb'
    # perfiod of collecting data in second
    collect_period = 10

# Run

    nohup ./collector.sh &
