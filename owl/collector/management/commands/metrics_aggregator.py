import json
import logging
import sys

from monitor import dbutil
from monitor.models import Table, HBaseCluster

# TODO: move these suffix definition to monitor/metric_help.py
OPERATION_NUM_OPS = 'NumOps'
OPERATION_AVG_TIME = 'AvgTime'
OPERATION_MIN_TIME = 'MinTime'
OPERATION_MAX_TIME = 'MaxTime'
OPERATION_TOTAL_TIME = 'TotalTime'

logger = logging.getLogger(__name__)

def make_empty_operation_metric():
  operationMetric = {}
  operationMetric[OPERATION_NUM_OPS] = 0
  operationMetric[OPERATION_TOTAL_TIME] = 0
  operationMetric[OPERATION_MAX_TIME] = 0
  operationMetric[OPERATION_MIN_TIME] = sys.maxint
  return operationMetric

def aggregate_one_region_operation_metric(aggregateMetric, deltaMetric):
  if OPERATION_NUM_OPS in deltaMetric:
    aggregateMetric[OPERATION_NUM_OPS] += deltaMetric[OPERATION_NUM_OPS]
    aggregateMetric[OPERATION_TOTAL_TIME] += (deltaMetric[OPERATION_AVG_TIME]
      * deltaMetric[OPERATION_NUM_OPS])
    if aggregateMetric[OPERATION_MAX_TIME] < deltaMetric[OPERATION_MAX_TIME]:
      aggregateMetric[OPERATION_MAX_TIME] = deltaMetric[OPERATION_MAX_TIME]
    if aggregateMetric[OPERATION_MIN_TIME] > deltaMetric[OPERATION_MIN_TIME]:
      aggregateMetric[OPERATION_MIN_TIME] = deltaMetric[OPERATION_MIN_TIME]

def compute_avg_time_and_num_ops_after_aggregation(operationMetrics):
  for operationName in operationMetrics.keys():
    if operationMetrics[operationName][OPERATION_NUM_OPS] > 0:
      # now, region operation metric will be collect every 10 seconds,
      # the orignal ops is the sum of ops during 10 seconds
      operationMetrics[operationName][OPERATION_AVG_TIME] = \
        (operationMetrics[operationName][OPERATION_TOTAL_TIME]
          / operationMetrics[operationName][OPERATION_NUM_OPS])
      operationMetrics[operationName][OPERATION_NUM_OPS] = \
        operationMetrics[operationName][OPERATION_NUM_OPS] / 10
    else:
      operationMetrics[operationName][OPERATION_AVG_TIME] = 0

def aggregate_region_operation_metric_in_process(output_queue, task_data):
  allClusterOperationMetric = {}
  # because the number of regions could be huge. We read out region operation metrics
  # by table, then table operation metrics and cluster operation metrics could be aggregated
  tables = Table.objects.all()
  for table in tables:
    clusterName = table.cluster.name
    clusterOperationMetric = allClusterOperationMetric.setdefault(clusterName, {})
    tableOperationMetric = {}
    regions = dbutil.get_region_by_table(table)
    logger.info(
      "TableOperationMetricAggregation aggregate %d " \
      "regions metric for table %s, cluster %s", len(regions),
      table.name, clusterName)

    for region in regions:
      if region.operationMetrics is None or region.operationMetrics == '':
        continue;
      regionOperationMetrics = json.loads(region.operationMetrics)
      for regionOperationName in regionOperationMetrics.keys():
        regionOperation = regionOperationMetrics[regionOperationName]
        aggregate_one_region_operation_metric(
          tableOperationMetric.setdefault(regionOperationName,
          make_empty_operation_metric()), regionOperation)
        aggregate_one_region_operation_metric(
          clusterOperationMetric.setdefault(regionOperationName,
          make_empty_operation_metric()), regionOperation)

    # compute avgTime for table operation metrics
    compute_avg_time_and_num_ops_after_aggregation(tableOperationMetric)
    table.operationMetrics = json.dumps(tableOperationMetric)
    table.save()

  # compute avgTime for clusetr operation metrics
  clusters = HBaseCluster.objects.all()
  for cluster in clusters:
    clusterName = cluster.cluster.name
    if clusterName in allClusterOperationMetric:
      clusterOperationMetric = allClusterOperationMetric[clusterName]
      compute_avg_time_and_num_ops_after_aggregation(clusterOperationMetric)
      cluster.operationMetrics = json.dumps(clusterOperationMetric)
      cluster.save()
  return


