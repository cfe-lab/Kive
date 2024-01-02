# Django settings for server project.

# Copy this file to settings.py, then set environment variables when you don't
# like the default values. Search for os.environ.get(name, default) to see all
# the options.
# Some common environment variables to change:
# KIVE_DEBUG: Turn this off in production! It gives more detailed error
#   messages, and provides other helpful features, like automatically
#   reloading when the source code changes.
# KIVE_SECRET_KEY: Encrypts session data.
# DATABASES: KIVE_DB_NAME, KIVE_DB_USER, and KIVE_DB_PASSWORD are site-specific.
# KIVE_MEDIA_ROOT: set to the absolute path you wish to use on your system
# KIVE_STATIC_ROOT: set to the absolute path you wish to use on your system
# KIVE_ALLOWED_HOSTS: the server's IP address to listen on
# KIVE_EMAIL_*:
#    settings used for sending logged error messages via email to the administrators
# KIVE_ADMINS: system administrators
# KIVE_LOG: log file to write to
# KIVE_PURGE_*: adjust the levels for when to purge old files
import os
import json

from django.core.management.utils import get_random_secret_key

# Turn this off in production!
DEBUG = os.environ.get("KIVE_DEBUG", 'True').lower() != 'false'

# This is a list of lists in JSON, for example:
# export KIVE_ADMINS='[["Your Name", "your_email@example.com"]]'
ADMINS = json.loads(os.environ.get("KIVE_ADMINS", "[]"))

EMAIL_HOST = os.environ.get("KIVE_EMAIL_HOST", "localhost")
SERVER_EMAIL = os.environ.get("KIVE_SERVER_EMAIL", "")
EMAIL_SUBJECT_PREFIX = os.environ.get("KIVE_SUBJECT_PREFIX", "[Kive] ")

AUTH_USER_MODEL = "auth.User"

MANAGERS = ADMINS

DATABASES = {
    'default': {
        # Engine can be 'postgresql_psycopg2', 'mysql', 'sqlite3' or 'oracle'.
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': os.environ.get('KIVE_DB_NAME', 'kive'),  # Or path to db file if using sqlite3.
        # The following settings are not used with sqlite3:
        'USER': os.environ.get('KIVE_DB_USER', ''),
        'PASSWORD': os.environ.get('KIVE_DB_PASSWORD', ''),
        # Blank host for localhost through domain sockets,
        # or '127.0.0.1' for localhost through TCP.
        'HOST': os.environ.get("KIVE_DB_HOST", ''),
        'PORT': ''  # Set to empty string for default.
    }
}

# Hosts/domain names that are valid for this site; required if DEBUG is False
# See https://docs.djangoproject.com/en/1.11/ref/settings/#allowed-hosts
# This is a list of strings in JSON, for example:
# export KIVE_ALLOWED_HOSTS='["localhost", "127.0.0.1"]'
ALLOWED_HOSTS = json.loads(os.environ.get("KIVE_ALLOWED_HOSTS", "[]"))

# Local time zone for this installation. Choices can be found here:
# http://en.wikipedia.org/wiki/List_of_tz_zones_by_name
# although not all choices may be available on all operating systems.
# In a Windows environment this must be set to your system time zone.
TIME_ZONE = 'America/Vancouver'

# Language code for this installation. All choices can be found here:
# http://www.i18nguy.com/unicode/language-identifiers.html
LANGUAGE_CODE = 'en-us'

SITE_ID = 1

# If you set this to False, Django will make some optimizations so as not
# to load the internationalization machinery.
USE_I18N = True

# If you set this to False, Django will not use timezone-aware datetimes.
USE_TZ = True

# Absolute filesystem path to the directory that will hold user-uploaded files.
# Example: "/var/www/example.com/media/"
MEDIA_ROOT = os.environ.get('KIVE_MEDIA_ROOT', os.path.expanduser('~/data/kive'))

# URL that handles the media served from MEDIA_ROOT. Make sure to use a
# trailing slash.
# Examples: "http://example.com/media/", "http://media.example.com/"
MEDIA_URL = ''

# Absolute path to the directory static files should be collected to.
# Don't put anything in this directory yourself; store your static files
# in apps' "static/" subdirectories and in STATICFILES_DIRS.
# Example: "/var/www/example.com/static/"
STATIC_ROOT = os.environ.get(
    'KIVE_STATIC_ROOT',
    os.path.abspath(os.path.join(__file__,
                                 '../../../static_root')))

# URL prefix for static files.
# Example: "http://example.com/static/", "http://static.example.com/"
STATIC_URL = '/static/'

# Additional locations of static files
STATICFILES_DIRS = (
    # Put strings here, like "/home/html/static" or "C:/www/django/static".
    # Always use forward slashes, even on Windows.
    # Don't forget to use absolute paths, not relative paths.
)

# List of finder classes that know how to find static files in
# various locations.
STATICFILES_FINDERS = (
    'django.contrib.staticfiles.finders.FileSystemFinder',
    'django.contrib.staticfiles.finders.AppDirectoriesFinder',
    # 'django.contrib.staticfiles.finders.DefaultStorageFinder',
)

# Add hashes to the names of static-files (to enable long cache times and
# avoid stale JS/CSS).
STATICFILES_STORAGE = "django.contrib.staticfiles.storage.ManifestStaticFilesStorage"

# Make this unique, and don't share it with anybody. Call this to generate a
# new one, then set environment variable:
# ./manage.py shell -c "import django; print(django.core.management.utils.get_random_secret_key())"
SECRET_KEY = os.environ.get('KIVE_SECRET_KEY')
IS_RANDOM_KEY = SECRET_KEY is None
if IS_RANDOM_KEY:
    SECRET_KEY = get_random_secret_key()

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
            'debug': DEBUG
        }
    },
]

MIDDLEWARE = (
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    # Uncomment the next line for simple clickjacking protection:
    # 'django.middleware.clickjacking.XFrameOptionsMiddleware',
)

ROOT_URLCONF = 'kive.urls'

# Python dotted path to the WSGI application used by Django's runserver.
WSGI_APPLICATION = 'kive.wsgi.application'

INSTALLED_APPS = (
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.sites',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    # Uncomment the next line to enable the admin:
    'django.contrib.admin',
    # Uncomment the next line to enable admin documentation:
    # 'django.contrib.admindocs',
    'metadata',
    'archive',
    'container',
    'librarian',
    'method',
    'pipeline',
    'transformation',
    'datachecking',
    'sandbox',
    'portal.apps.PortalConfig',
    'stopwatch',
    'fleet',
    'rest_framework',
)

LOG_HANDLERS = {
    'mail_admins': {
        'level': 'ERROR',
        'filters': ['require_debug_false', 'rate_limit'],
        'class': 'django.utils.log.AdminEmailHandler'
    },
    'console': {
        'level': 'DEBUG',
        'class': 'logging.StreamHandler',
        'formatter': 'debug'
    }
}
LOG_HANDLER_NAMES = []
LOG_FILE = os.environ.get('KIVE_LOG')
if LOG_FILE:
    LOG_HANDLER_NAMES.append('file')
    LOG_HANDLERS['file'] = {
        'level': 'DEBUG',
        'class': 'logging.handlers.RotatingFileHandler',
        'filename': LOG_FILE,
        'formatter': 'debug',
        'maxBytes': 1024*1024*15,  # 15MB
        'backupCount': 10
    }
else:
    LOG_HANDLER_NAMES.append('console')
if ADMINS:
    LOG_HANDLER_NAMES.append('mail_admins')
LOG_LEVEL = os.environ.get('KIVE_LOG_LEVEL', 'WARNING')

# See http://docs.djangoproject.com/en/dev/topics/logging for
# more details on how to customize your logging configuration.
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'filters': {
        'require_debug_false': {
            '()': 'django.utils.log.RequireDebugFalse'},
        'rate_limit': {'()': 'kive.ratelimitingfilter.RateLimitingFilter',
                       'rate': 1,
                       'per': 300,  # seconds
                       'burst': 5}
    },
    'formatters': {
        'debug': {
            'format': '%(asctime)s[%(levelname)s]%(name)s.%(funcName)s(): %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S'
        }
    },
    'handlers': LOG_HANDLERS,
    'root': {
        # This is the default logger.
        'handlers': LOG_HANDLER_NAMES,
        'level': LOG_LEVEL
    },
    'loggers': {
        # Change the logging level for an individual logger.
        # 'archive.tests': {
        #      'level': 'DEBUG'
        # },
        "fleet.Manager": {
            "level": "INFO",
        },
        "fleet.Worker": {
            "level": "INFO",
        }
    }
}

REST_FRAMEWORK = {
    'UPLOADED_FILES_USE_URL': False
}

FILE_UPLOAD_PERMISSIONS = 0o644

TEST_RUNNER = 'django.test.runner.DiscoverRunner'

LOGIN_URL = "login"
LOGIN_REDIRECT_URL = "home"

# Settings for the purge task. How much storage triggers a purge, and how much
# will stop the purge.
PURGE_START = os.environ.get('KIVE_PURGE_START', '20GB')
PURGE_STOP = os.environ.get('KIVE_PURGE_STOP', '15GB')
# How fast the different types of storage get purged. Higher aging gets purged faster.
PURGE_DATASET_AGING = os.environ.get('KIVE_PURGE_DATASET_AGING', '1.0')
PURGE_LOG_AGING = os.environ.get('KIVE_PURGE_LOG_AGING', '10.0')
PURGE_SANDBOX_AGING = os.environ.get('KIVE_PURGE_SANDBOX_AGING', '10.0')
# How long to wait before purging a file with no entry in the database.
# This gets parsed by django.utils.dateparse.parse_duration().
PURGE_WAIT = os.environ.get('KIVE_PURGE_WAIT', '0 days, 1:00:00')
PURGE_BATCH_SIZE = int(os.environ.get('KIVE_PURGE_BATCH_SIZE', '100'))

# A list, ordered from lowest-priority to highest-priority, of Slurm queues to
# be used by Kive.  Fill these in with the names of the queues as you have them
# defined on your system.  The tuples contain the name Kive will use for the
# queue as well as the actual Slurm name.
SLURM_QUEUES = json.loads(os.environ.get("KIVE_SLURM_QUEUES", """\
[
    ["Low priority", "LOW_PRIO"],
    ["Medium priority", "MEDIUM_PRIO"],
    ["High priority", "HIGH_PRIO"]
]
"""))

# The number of times to retry a slurm command such as sbatch or sacct,
# and the interval in seconds to wait between retries.
SLURM_COMMAND_RETRY_NUM = int(
    os.environ.get('KIVE_SLURM_COMMAND_RETRY_NUM', '10'))
SLURM_COMMAND_RETRY_SLEEP_SECS = int(
    os.environ.get('KIVE_SLURM_COMMAND_RETRY_SLEEP_SECS', '10'))

# Add items to the PATH environment variable before launching any slurm jobs.
# Often useful to choose a Python virtual environment.
SLURM_PATH = os.environ.get('KIVE_SLURM_PATH')

# Container file in Containers folder
DEFAULT_CONTAINER = os.environ.get('KIVE_DEFAULT_CONTAINER', 'kive-default.simg')
DEFAULT_AUTO_FIELD = 'django.db.models.AutoField'
