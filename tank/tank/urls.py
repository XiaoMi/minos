from django.conf.urls import patterns, include, url
from django.conf.urls.static import static
from django.conf import settings
from package_server.views import check_package
from package_server.views import get_latest_package_info
from package_server.views import list_packages
from package_server.views import upload_package

# Uncomment the next two lines to enable the admin:
from django.contrib import admin
admin.autodiscover()

urlpatterns = patterns('',
    # Examples:
    # url(r'^$', 'tank.views.home', name='home'),
    # url(r'^tank/', include('tank.foo.urls')),

    # Uncomment the admin/doc line below to enable admin documentation:
    # url(r'^admin/doc/', include('django.contrib.admindocs.urls')),

    # Uncomment the next line to enable the admin:
    # url(r'^admin/', include(admin.site.urls)),
    url(r'^$' , list_packages),
    url(r'^package_list/(\d*)$', list_packages),
    url(r'^upload_package/$', upload_package),
    url(r'^check_package/$', check_package),
    url(r'^get_latest_package_info/$', get_latest_package_info),
) + (static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
  + static(settings.STATIC_URL, document_root=settings.STATIC_ROOT))
