# -*- coding: utf-8 -*-

from django.conf.urls import patterns, url
import views

urlpatterns = patterns(
  '',
  url(r'^$', views.index),
  url(r'^longhaul/(?P<id>\d+)/$', views.show_longhaul),
)
