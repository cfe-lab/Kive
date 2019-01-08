# Django settings for server project.

# Copy this file to settings.py, then adjust the settings in the copy.
# Kive-specific things to set:
# DATABASES: NAME, USER, and PASSWORD are site-specific
# MEDIA_ROOT: set to the absolute path you wish to use on your system
# STATIC_ROOT: set to the absolute path you wish to use on your system
# KIVE_SANDBOX_WORKER_ACCOUNT: the user account used to run sandboxes
# KIVE_PROCESSING_GROUP: group representing users that have access to the sandboxes
# EMAIL_{HOST|PORT|HOST_USER|HOST_PASSWORD|USE_TLS|USE_SSL|TIMEOUT|SSL_KEYFILE|SSL_CERTFILE}:
#    settings used for sending logged error messages via email to the administrators
# ADMINS: system administrators
import os
import json

from django.core.management.utils import get_random_secret_key

DEBUG = os.environ.get("KIVE_DEBUG", True)

ADMINS = (
    # ('Your Name', 'your_email@example.com'),
)
raw_admins = os.environ.get("KIVE_ADMINS")
if raw_admins is not None:
    ADMINS = json.loads(raw_admins)

# These are the default values; customize for your installation.
# EMAIL_HOST = "localhost"
# EMAIL_PORT = 25
# SERVER_EMAIL = ""
# EMAIL_HOST_USER = ""
# EMAIL_HOST_PASSWORD = ""
# EMAIL_USE_TLS = False
# EMAIL_USE_SSL = False
# EMAIL_TIMEOUT = None
# EMAIL_SUBJECT_PREFIX = "[Kive] "
# EMAIL_SSL_CERTFILE = None
# EMAIL_SSL_KEYFILE = None
# EMAIL_BACKEND = "django.core.mail.backends.smtp.EmailBackend"

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
# See https://docs.djangoproject.com/en/1.7/ref/settings/#allowed-hosts
ALLOWED_HOSTS = []
raw_allowed_hosts = os.environ.get("KIVE_ALLOWED_HOSTS")  # this must be a JSON list string
if raw_allowed_hosts is not None:
    ALLOWED_HOSTS = json.loads(raw_allowed_hosts)

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

# If you set this to False, Django will not format dates, numbers and
# calendars according to the current locale.
USE_L10N = True

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

# Make this unique, and don't share it with anybody. Call
# get_random_secret_key() to generate a new one, then set environment variable.
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
                'django.contrib.auth.context_processors.auth'
            ],
            'debug': DEBUG
        }
    },
]

MIDDLEWARE_CLASSES = (
    'django.middleware.common.CommonMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
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
LOG_LEVEL = os.environ.get('KIVE_LOG_LEVEL', 'WARN')

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

# The polling interval that the manager of the fleet uses between queries to the database.
FLEET_POLLING_INTERVAL = 30  # in seconds
FLEET_PURGING_INTERVAL = 3600  # in seconds

# The time interval the worker uses between polling for progress, in seconds.
# Shorter sleep makes worker more responsive, generates more load when idle.
SLEEP_SECONDS = 0.1

# External files are held on a remote file system beyond Kive's control; When the
# Kive Manager is idle, she will periodically check to see whether these files still
# exist where they were when they were added to the system.
# If a file is found to be missing, an error is issued via the logging system.
# Here, set how often each external file should be checked
# Setting these variables to zero will disable the checking system.
EXTERNAL_FILE_CHECK_DAYS = 0
EXTERNAL_FILE_CHECK_HOURS = 0
EXTERNAL_FILE_CHECK_MINUTES = 0

TEST_RUNNER = 'django.test.runner.DiscoverRunner'

LOGIN_URL = "login"
LOGIN_REDIRECT_URL = "home"

# Sandbox configuration

# The path to the directory that sandboxes run in.  If this is not an absolute path,
# sandboxes will run in ${MEDIA_ROOT}/${SANDBOX_PATH}.  Otherwise it will run in
# exactly this path.
SANDBOX_PATH = "Sandboxes"

# Settings for the purge task. How much storage triggers a purge, and how much
# will stop the purge.
PURGE_START = os.environ.get('KIVE_PURGE_START', '20 GB')
PURGE_STOP = os.environ.get('KIVE_PURGE_STOP', '15 GB')
# How fast the different types of storage get purged. Higher aging gets purged faster.
PURGE_DATASET_AGING = os.environ.get('KIVE_PURGE_DATASET_AGING', '1.0')
PURGE_LOG_AGING = os.environ.get('KIVE_PURGE_LOG_AGING', '10.0')
PURGE_CONTAINER_AGING = os.environ.get('KIVE_PURGE_CONTAINER_AGING', '10.0')
# How long to wait before purging a file with no entry in the database.
# This gets parsed by django.utils.dateparse.parse_duration().
PURGE_WAIT = os.environ.get('KIVE_PURGE_WAIT', '0 days, 1:00')

# Here you specify the time that sandboxes should be left after finishing
# before being automatically purged. (They may still be manually purged
# sooner than this.)  These quantities get added up.
SANDBOX_PURGE_DAYS = 1
SANDBOX_PURGE_HOURS = 0
SANDBOX_PURGE_MINUTES = 0

# Whether the fleet Manager should run idle tasks.
DO_IDLE_TASKS = True

# try to run an idle task every IDLE_TASK_FACTOR * FLEET_POLLING_INTERVAL
IDLE_TASK_FACTOR = 50

# Keep this many of the most recent Sandboxes for any PipelineFamily.
SANDBOX_KEEP_RECENT = 10

# When to start purging old output datasets
DATASET_MAX_STORAGE = 5 << 40  # TB
# When to stop purging
DATASET_TARGET_STORAGE = 2 << 40  # TB

# Only dataset files older than this period will be considered for purging.
DATASET_GRACE_PERIOD_HRS = 1.0

# Set the frequency with which the dataset directory is rescanned for files to purge.
DATASET_PURGE_SCAN_PERIOD_HRS = 12.0

# When to start purging old logfiles
LOGFILE_MAX_STORAGE = 5 << 40  # TB
# When to stop purging
LOGFILE_TARGET_STORAGE = 2 << 40  # TB

# only log files older than this period will be considered for purging
LOGFILE_GRACE_PERIOD_HRS = 1.0

# set the frequency with which the Log directory is rescanned for files to purge.
LOGFILE_PURGE_SCAN_PERIOD_HRS = 12.0

# Worker account configuration

# The system user that actually runs the code.  This user:
# - should be a standard unprivileged user
# - should have access to the sandbox directory
# - should be made the owner of all sandboxes
# - should be accessible via SSH by the Kive user (i.e. apache on a production machine,
#   your user account on a developer machine) without password.
# - should have access to any tools used in any of your CodeResources on PATH
# - should use bash as its default shell
# Leave blank to run as the user that launches the fleet.
KIVE_SANDBOX_WORKER_ACCOUNT = ""

# The system group that contains both the user that launches the fleet and
# the sandbox worker account.  This is ignored if KIVE_SANDBOX_WORKER_ACCOUNT is blank.
KIVE_PROCESSING_GROUP = os.environ.get("KIVE_PROCESSING_GROUP", "")

# Number of rows to display on the View Dataset page.
DATASET_DISPLAY_MAX = 100

# A list, ordered from lowest-priority to highest-priority, of Slurm queues to
# be used by Kive.  Fill these in with the names of the queues as you have them
# defined on your system.  The tuples contain the name Kive will use for the
# queue as well as the actual Slurm name.
SLURM_QUEUES = [
    ("Low priority", "LOW_PRIO"),
    ("Medium priority", "MEDIUM_PRIO"),
    ("High priority", "HIGH_PRIO")
]
# Number of seconds between checking Slurm for job information.
DEFAULT_SLURM_CHECK_INTERVAL = 5

KIVE_HOME = os.path.abspath(os.path.join(__file__, '../..'))
STEP_HELPER_COMMAND = "step_helper"
CABLE_HELPER_COMMAND = "cable_helper"

# Steps in Pipelines will run as Slurm tasks, using a script like this:
"""
#!/usr/bin/env bash

[SANDBOX_XXX_PREAMBLE]

[DRIVER] [INPUTS] [OUTPUTS]
"""
# If there's anything system-specific that you need in that spot, put it
# into this variable:
SANDBOX_SETUP_PREAMBLE = os.environ.get("KIVE_SANDBOX_SETUP_PREAMBLE", "")
SANDBOX_DRIVER_PREAMBLE = os.environ.get("KIVE_SANDBOX_DRIVER_PREAMBLE", "")
SANDBOX_BOOKKEEPING_PREAMBLE = os.environ.get("KIVE_SANDBOX_BOOKKEEPING_PREAMBLE", "")
SANDBOX_CABLE_PREAMBLE = os.environ.get("KIVE_SANDBOX_CABLE_PREAMBLE", "")

# The amount of memory to allocate to setup, bookkeeping, and cable tasks (in MB).
SANDBOX_SETUP_MEMORY = os.environ.get("KIVE_SANDBOX_SETUP_MEMORY", 100)
SANDBOX_BOOKKEEPING_MEMORY = os.environ.get("KIVE_SANDBOX_BOOKKEEPING_MEMORY", 100)
SANDBOX_CABLE_MEMORY = os.environ.get("KIVE_SANDBOX_CABLE_MEMORY", 100)

CONFIRM_COPY_RETRIES = 5
CONFIRM_COPY_WAIT_MIN = 3
CONFIRM_COPY_WAIT_MAX = 7

CONFIRM_FILE_CREATED_RETRIES = 5
CONFIRM_FILE_CREATED_WAIT_MIN = 8
CONFIRM_FILE_CREATED_WAIT_MAX = 12

# The settings file the fleet should use.  Leave as None if it should just use the normal one.
FLEET_SETTINGS = None

# The keyword used by the system's sinfo command to retrieve a queue's priority,
# and the name of the corresponding column returned.
SLURM_PRIO_KEYWORD = os.environ.get('KIVE_PRIO_KEYWORD', "prioritytier")
SLURM_PRIO_COLNAME = os.environ.get('KIVE_PRIO_COLNAME', "PRIO_TIER")

# Attempt to run the system tests that use Slurm.
RUN_SLURM_TESTS = os.environ.get('KIVE_RUN_SLURM_TESTS', 'False').lower() != 'false'

# The number of times to retry a slurm command such as sbatch or sacct,
# and the interval in seconds to wait between retries.
# NOTE: these timeouts are also used for DOCKER_COMMAND.
SLURM_COMMAND_RETRY_NUM = 10
SLURM_COMMAND_RETRY_SLEEP_SECS = 10

# Fail any slurm job that reports a NODE_FAIL for longer than this time (in seconds).
NODE_FAIL_TIME_OUT_SECS = 5*60

# Attempt to run the system tests that use Docker.
RUN_DOCKER_TESTS = os.environ.get('KIVE_RUN_DOCKER_TESTS', 'False').lower() != 'false'

DOCK_DOCKER_COMMAND = "/usr/bin/docker"
DOCK_BZIP2_COMMAND = "/bin/bzip2"

# Attempt to run the system tests that use singularity
# NOTE: It only makes sense to have this true iff RUN_DOCKER_TESTS is also true
RUN_SINGULARITY_TESTS = (
        RUN_DOCKER_TESTS and
        os.environ.get('KIVE_RUN_SINGULARITY_TESTS', 'False').lower() != 'false')

# Container file in CodeResources folder
DEFAULT_CONTAINER = 'kive-default.simg'
