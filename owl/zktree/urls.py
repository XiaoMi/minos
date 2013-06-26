from django.conf.urls import patterns, url
import views

urlpatterns = patterns(
  '',
  url(r'^(?P<addrs>[^/]+)/(?P<path>.*)/$',views.index),
  url(r'^(?P<addrs>.+)/$',views.index),
)
