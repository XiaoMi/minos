# -*- coding: utf-8 -*-

import datetime
import httplib
import time

from django.conf import settings
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
from django.http import HttpResponse
from django.shortcuts import render_to_response
from django.template import RequestContext, Context, loader

from models import Action
from models import Task

# /failover/
def index(request):

  current_time = time.time() * 1000 # in ms
  now = datetime.datetime.now()
  today_first_timestamp = current_time - ((now.hour*60 + now.minute)*60 + now.second)*1000
  previous_hour = current_time - 3600000 # 60*60*1000
  previous_day = current_time - 86400000 # 24*60*60*1000
  previous_week = current_time - 604800000 # 7*24*60*60*1000
  previous_month = current_time - 2592000000 # 30*24*60*60*100
  previous_year = current_time - 31536000000 # 365*24*60*60*1000

  hour_task_number = Task.objects.filter(start_timestamp__gt=previous_hour).count()
  hour_fail_task_number = Task.objects.filter(start_timestamp__gt=previous_hour, success=False).count()
  hour_action_number = Action.objects.filter(task_id__gt=previous_hour).count()
  hour_fail_action_number = Action.objects.filter(task_id__gt=previous_hour, success=False).count()
  day_task_number = Task.objects.filter(start_timestamp__gt=previous_day).count()
  day_fail_task_number = Task.objects.filter(start_timestamp__gt=previous_day, success=False).count()
  day_action_number = Action.objects.filter(task_id__gt=previous_day).count()
  day_fail_action_number = Action.objects.filter(task_id__gt=previous_day, success=False).count()
  week_task_number = Task.objects.filter(start_timestamp__gt=previous_week).count()
  week_fail_task_number = Task.objects.filter(start_timestamp__gt=previous_week, success=False).count()
  week_action_number = Action.objects.filter(task_id__gt=previous_week).count()
  week_fail_action_number = Action.objects.filter(task_id__gt=previous_week, success=False).count()
  month_task_number = Task.objects.filter(start_timestamp__gt=previous_month).count()
  month_fail_task_number = Task.objects.filter(start_timestamp__gt=previous_month, success=False).count()
  month_action_number = Action.objects.filter(task_id__gt=previous_month).count()
  month_fail_action_number = Action.objects.filter(task_id__gt=previous_month, success=False).count()
  year_task_number = Task.objects.filter(start_timestamp__gt=previous_year).count()
  year_fail_task_number = Task.objects.filter(start_timestamp__gt=previous_year, success=False).count()
  year_action_number = Action.objects.filter(task_id__gt=previous_year).count()
  year_fail_action_number = Action.objects.filter(task_id__gt=previous_year, success=False).count()
  total_task_number = Task.objects.count()
  total_fail_task_number = Task.objects.filter(success=False).count()
  total_action_number = Action.objects.count()
  total_fail_action_number = Action.objects.filter(success=False).count()

  today_tasks = Task.objects.filter(start_timestamp__gt=today_first_timestamp)
  context = {
    "chart_id": "today_tasks",
    "chart_title": "Today Tasks",
    "tasks": today_tasks,
    }
  failover_task_chart = loader.get_template("failover_task_chart.tpl").render(Context(context))

  host = settings.FAILOVER_FRAMEWORK_HOST
  port = settings.FAILOVER_FRAMEWORK_PORT
  host_port = host + ":" + str(port)

  try:
    conn = httplib.HTTPConnection(host_port)
    conn.request('HEAD', "/")
    response = conn.getresponse()
    conn.close()
    is_running = response.status == 200
  except:
    is_running = False
  
  context = {
    "failover_task_chart": failover_task_chart,
    "is_running": is_running,
    "host_port": host_port,
    "hour_task_number": hour_task_number,
    "hour_fail_task_number": hour_fail_task_number,
    "hour_action_number": hour_action_number,
    "hour_fail_action_number": hour_fail_action_number,
    "day_task_number": day_task_number,
    "day_fail_task_number": day_fail_task_number,
    "day_action_number": day_action_number,
    "day_fail_action_number": day_fail_action_number,
    "week_task_number": week_task_number,
    "week_fail_task_number": week_fail_task_number,
    "week_action_number": week_action_number,
    "week_fail_action_number": week_fail_action_number,
    "month_task_number": month_task_number,
    "month_fail_task_number": month_fail_task_number,
    "month_action_number": month_action_number,
    "month_fail_action_number": month_fail_action_number,
    "year_task_number": year_task_number,
    "year_fail_task_number": year_fail_task_number,
    "year_action_number": year_action_number,
    "year_fail_action_number": year_fail_action_number,
    "total_task_number": total_task_number,
    "total_fail_task_number": total_fail_task_number,
    "total_action_number": total_action_number,
    "total_fail_action_number": total_fail_action_number,
    }
  return render_to_response("index.html", context, context_instance=RequestContext(request))

def paging_objects(request, objects, number):
  paginator = Paginator(objects, number)
  page = request.GET.get("page")
  try:
    objects_to_show = paginator.page(page)
  except PageNotAnInteger:
    objects_to_show = paginator.page(1)
  except EmptyPage:
    objects_to_show = paginator.page(page.num_pages)
  return objects_to_show

# /failover/task/
def show_tasks(request):
  
  # ?latest=10
  if request.GET.get("latest"):
    number = request.GET.get("latest")
    # ?latest=10&fail=Ture
    if request.GET.get("fail"):
      tasks = Task.objects.filter(success=False).order_by("start_timestamp").reverse()[:number]
    else:
      tasks = Task.objects.all().order_by("start_timestamp").reverse()[:number]
  # ?start_time=2013-09-11%2017:51:22
  elif request.GET.get("start_time"):
    tasks = Task.objects.filter(start_time=request.GET.get("start_time"))
  # no params
  else:
    tasks = Task.objects.all().order_by("start_timestamp").reverse()   

  tasks_to_show = paging_objects(request, tasks, 20)

  context = {
    "tasks": tasks_to_show,
    }
  return render_to_response("show_tasks.html", context, context_instance=RequestContext(request))


# /failover/action/
def show_actions(request):
  
  # ?latest=10
  if request.GET.get("latest"):
    number = request.GET.get("latest")
    # ?latest=10&fail=True
    if request.GET.get("fail"):
      actions = Action.objects.filter(success=False).order_by("task").reverse()[:number]
    else:
      actions = Action.objects.all().order_by("task").reverse()[:number]
  # ?start_time=2013-09-11_%2017:51:22
  elif request.GET.get("start_time"):
    actions = Action.objects.filter(start_time=request.GET.get("start_time"))
  else:
    actions = Action.objects.all().order_by("task").reverse()
        
  actions_to_show = paging_objects(request, actions, 20)

  context = {
    "actions": actions_to_show,
    }
  return render_to_response("show_actions.html", context, context_instance=RequestContext(request))

