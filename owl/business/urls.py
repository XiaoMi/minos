# -*- coding: utf-8 -*-

from django.conf.urls import patterns, url
import views

urlpatterns = patterns(
  '',
  url(r'^$', views.index),
  url(r'(?P<id>1)/(?P<access_type>[^/]+)/(?P<label>[^/]+)', views.show_online),
  url(r'(?P<id>2)/(?P<access_type>[^/]+)/(?P<label>[^/]+)', views.show_business),
)
