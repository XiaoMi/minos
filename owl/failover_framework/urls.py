# -*- coding: utf-8 -*-

from django.conf.urls import patterns, url
import views

urlpatterns = patterns(
  '',
  url(r'^$', views.index),
  url(r'^action/', views.show_actions),
  url(r'^task/', views.show_tasks),
)
