# -*- coding: utf-8 -*-

from django.db import models

class Task(models.Model):
  start_timestamp =models.BigIntegerField(primary_key=True)
  start_time = models.CharField(max_length=64)
  action_number = models.IntegerField()
  cluster_healthy = models.BooleanField()
  data_consistent = models.BooleanField()
  success = models.BooleanField()
  
class Action(models.Model):
  task = models.ForeignKey(Task)
  start_time = models.CharField(max_length=64)
  name = models.CharField(max_length=256)
  success = models.BooleanField()
  consume_time = models.IntegerField()
  
