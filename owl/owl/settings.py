# Django settings for owl project.

import os, django
# calculated paths for django and the site
# used as starting points for various other paths
DJANGO_ROOT = os.path.dirname(os.path.realpath(django.__file__))
SITE_ROOT = os.path.dirname(os.path.realpath(__file__))

DEBUG = False
TEMPLATE_DEBUG = DEBUG
ALLOWED_HOSTS = ['*']

ADMINS = (
  ('admin', ''),
)

MANAGERS = ADMINS

DATABASES = {
  'default': {
    'ENGINE': 'django.db.backends.mysql', # Add 'postgresql_psycopg2', 'mysql', 'sqlite3' or 'oracle'.
    'NAME': 'owl', # Or path to database file if using sqlite3.
    'USER': 'owl', # Not used with sqlite3.
    'PASSWORD': 'owl', # Not used with sqlite3.
    'HOST': 'localhost', # Set to empty string for localhost. Not used with sqlite3.
    'PORT': '3306', # Set to empty string for default. Not used with sqlite3.
  }
}

# Local time zone for this installation. Choices can be found here:
# http://en.wikipedia.org/wiki/List_of_tz_zones_by_name
# although not all choices may be available on all operating systems.
# In a Windows environment this must be set to your system time zone.
TIME_ZONE = 'Asia/Shanghai'

# Language code for this installation. All choices can be found here:
# http://www.i18nguy.com/unicode/language-identifiers.html
LANGUAGE_CODE = 'en-us'

SITE_ID = 1

# If you set this to False, Django will make some optimizations so as not
# to load the internationalization machinery.
USE_I18N = True

# If you set this to False, Django will not format dates, numbers and
# calendars according to the current locale.
USE_L10N = True

# If you set this to False, Django will not use timezone-aware datetimes.
USE_TZ = True

# Absolute filesystem path to the directory that will hold user-uploaded files.
# Example: "/home/media/media.lawrence.com/media/"
MEDIA_ROOT = ''

# URL that handles the media served from MEDIA_ROOT. Make sure to use a
# trailing slash.
# Examples: "http://media.lawrence.com/media/", "http://example.com/media/"
MEDIA_URL = ''

# Absolute path to the directory static files should be collected to.
# Don't put anything in this directory yourself; store your static files
# in apps' "static/" subdirectories and in STATICFILES_DIRS.
# Example: "/home/media/media.lawrence.com/static/"
STATIC_ROOT = ''

# URL prefix for static files.
# Example: "http://media.lawrence.com/static/"
STATIC_URL = '/static/'

# Additional locations of static files
STATICFILES_DIRS = (
  os.path.join(SITE_ROOT, '../static'),
  # Put strings here, like "/home/html/static" or "C:/www/django/static".
  # Always use forward slashes, even on Windows.
  # Don't forget to use absolute paths, not relative paths.
  )

# List of finder classes that know how to find static files in
# various locations.
STATICFILES_FINDERS = (
  'django.contrib.staticfiles.finders.FileSystemFinder',
  'django.contrib.staticfiles.finders.AppDirectoriesFinder',
  #'django.contrib.staticfiles.finders.DefaultStorageFinder',
  )

# Make this unique, and don't share it with anybody.
SECRET_KEY = '#q8xc4y#r^u+7^lymd3^lg6%vips0hc-8&amp;#4^dncjr6p&amp;2rh@0'

# List of callables that know how to import templates from various sources.
TEMPLATE_LOADERS = (
  'django.template.loaders.filesystem.Loader',
  'django.template.loaders.app_directories.Loader',
  #     'django.template.loaders.eggs.Loader',
  )

MIDDLEWARE_CLASSES = (
  'django.middleware.common.CommonMiddleware',
  'django.contrib.sessions.middleware.SessionMiddleware',
  'django.middleware.csrf.CsrfViewMiddleware',
  'django.contrib.auth.middleware.AuthenticationMiddleware',
  'django.contrib.messages.middleware.MessageMiddleware',
  'django.middleware.transaction.TransactionMiddleware',
  # Uncomment the next line for simple clickjacking protection:
  # 'django.middleware.clickjacking.XFrameOptionsMiddleware',
  )

ROOT_URLCONF = 'owl.urls'

# Python dotted path to the WSGI application used by Django's runserver.
WSGI_APPLICATION = 'owl.wsgi.application'

TEMPLATE_DIRS = (os.path.join(SITE_ROOT, '../templates'))

INSTALLED_APPS = (
  'django.contrib.auth',
  'django.contrib.contenttypes',
  'django.contrib.sessions',
  'django.contrib.sites',
  'django.contrib.messages',
  'django.contrib.staticfiles',
  'django.contrib.humanize',
  # Uncomment the next line to enable the admin:
  'django.contrib.admin',
  # Uncomment the next line to enable admin documentation:
  # 'django.contrib.admindocs',
  'alert',
  'business',
  'collector',
  'hbase',
  'monitor',
  'quota',
  'zktree',
  'failover_framework',
  )

# A sample logging configuration. The only tangible logging
# performed by this configuration is to send an email to
# the site admins on every HTTP 500 error when DEBUG=False.
# See http://docs.djangoproject.com/en/dev/topics/logging for
# more details on how to customize your logging configuration.
LOGGING = {
  'version': 1,
  'disable_existing_loggers': False,
  'formatters': {
    'verbose': {
      'format': '%(levelname)s %(asctime)s %(module)s %(process)d %(thread)d %(message)s'
    },
    'simple': {
      'format': '%(levelname)s %(message)s'
    }
  },
  'filters': {
    'require_debug_false': {
      '()': 'django.utils.log.RequireDebugFalse'
    }
  },
  'handlers': {
    'null': {
      'level': 'DEBUG',
      'class': 'django.utils.log.NullHandler',
    },
    'console':{
      'level': 'DEBUG',
      'class': 'logging.StreamHandler',
      'formatter': 'verbose'
    },
    'mail_admins': {
      'level': 'ERROR',
      'filters': ['require_debug_false'],
      'class': 'django.utils.log.AdminEmailHandler'
    },
    'file':{
      'level': 'DEBUG',
      'class': 'logging.FileHandler',
      'filename': 'debug.log',
      'formatter': 'verbose'
    }
  },
  'loggers': {
    'django.request': {
      'handlers': ['mail_admins', 'file'],
      'level': 'ERROR',
      'propagate': True,
    },
    'alert': {
      'handlers': ['console'],
      'level': 'INFO',
      'propagate': True,
    },
    'collector': {
      'handlers': ['console'],
      'level': 'INFO',
      'propagate': True,
    },
    'quota': {
      'handlers': ['console'],
      'level': 'INFO',
      'propagate': True,
    },
    'monitor': {
      'handlers': ['console', 'file'],
      'level': 'INFO',
      'propagate': True,
    },
    'failover_framework': {
      'handlers': ['console'],
      'level': 'INFO',
      'propagate': True,
    }
  }
}

# site config
LOGIN_REDIRECT_URL = '/monitor/'

# for failover framework app
FAILOVER_FRAMEWORK_HOST = "127.0.0.1"
FAILOVER_FRAMEWORK_PORT = 9981
FAILOVER_FRAMEWORK_PERIOD = 600
