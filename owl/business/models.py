# -*- coding: utf-8 -*-

from django.db import models

class Business(models.Model):
  # The buisness name, sms/miliao/...
  business = models.CharField(max_length=32)
  # The hbase cluster name
  cluster = models.CharField(max_length=32)
  # The tables of business
  tables = models.CharField(max_length=32)
  # The server write or read data from hbase
  access_server = models.TextField()
  # The discription
  description = models.TextField()

  def getCounterGroup(self):
    return u"infra-hbase-business-%s" % (self.business)

  def __unicode__(self):
    return u"%s/%s" % (self.business, self.cluster)
