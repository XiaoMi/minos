from django.conf.urls import patterns, include, url
from django.views.generic import RedirectView

# Uncomment the next two lines to enable the admin:
from django.contrib import admin

admin.autodiscover()

urlpatterns = patterns(
  '',
  # Examples:
  url(r'^$', RedirectView.as_view(url='/monitor/')),
  # url(r'^owl/', include('owl.foo.urls')),

  # Uncomment the admin/doc line below to enable admin documentation:
  url(r'^admin/doc/', include('django.contrib.admindocs.urls')),

  # Uncomment the next line to enable the admin:
  url(r'^admin/', include(admin.site.urls)),

  url(r'^accounts/', include('django.contrib.auth.urls')),

  url(r'^monitor/', include('monitor.urls')),
  url(r'^hbase/', include('hbase.urls')),
  url(r'^business/', include('business.urls')),
  url(r'^zktree/', include('zktree.urls')),
  url(r'^failover/', include('failover_framework.urls')),
)
