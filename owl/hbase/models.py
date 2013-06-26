# -*- coding: utf-8 -*-

from django.db import models

class Longhaul(models.Model):
  # The cluster name
  cluster = models.CharField(max_length=32)
  # The table name
  table = models.CharField(max_length=32)
  # the column family of the long hual
  cf = models.CharField(max_length=32)
  # the load description of the longhaul test
  description = models.TextField()

  def getCounterGroup(self):
    return u"infra-hbase-longhaul-%s-%s-%s" % (self.cluster, self.table, self.cf)

  def __unicode__(self):
    return u"%s/%s" % (self.cluster, self.table)
