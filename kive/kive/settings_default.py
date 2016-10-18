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


DEBUG = True

ADMINS = (
    # ('Your Name', 'your_email@example.com'),
)

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

AUTH_USER_MODEL = "auth.User"

MANAGERS = ADMINS

DATABASES = {
    'default': {
        # Engine can be 'postgresql_psycopg2', 'mysql', 'sqlite3' or 'oracle'.
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': '[YOUR DB NAME HERE]',  # Or path to db file if using sqlite3.
        # The following settings are not used with sqlite3:
        'USER': '[YOUR DB USER NAME HERE]',
        'PASSWORD': '[YOUR DB USER PASSWORD HERE]',
        # Blank host for localhost through domain sockets,
        # or '127.0.0.1' for localhost through TCP.
        'HOST': '',
        'PORT': ''  # Set to empty string for default.
    }
}

# Hosts/domain names that are valid for this site; required if DEBUG is False
# See https://docs.djangoproject.com/en/1.7/ref/settings/#allowed-hosts
ALLOWED_HOSTS = []

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
MEDIA_ROOT = ''

# URL that handles the media served from MEDIA_ROOT. Make sure to use a
# trailing slash.
# Examples: "http://example.com/media/", "http://media.example.com/"
MEDIA_URL = ''

# Absolute path to the directory static files should be collected to.
# Don't put anything in this directory yourself; store your static files
# in apps' "static/" subdirectories and in STATICFILES_DIRS.
# Example: "/var/www/example.com/static/"
STATIC_ROOT = ''

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

# Make this unique, and don't share it with anybody.
SECRET_KEY = 'n+m#lw#z3!ertoh&vx_bs0)c9*z5)eadw0k_hpzp^&s@xbbkx$'

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
    'librarian',
    'method',
    'pipeline',
    'transformation',
    'datachecking',
    'sandbox',
    'portal',
    'stopwatch',
    'fleet',
    'rest_framework',
)

# See http://docs.djangoproject.com/en/dev/topics/logging for
# more details on how to customize your logging configuration.
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'filters': {
        'require_debug_false': {
            '()': 'django.utils.log.RequireDebugFalse'
        }
    },
    'formatters': {
        'debug': {
            'format': '%(asctime)s[%(levelname)s]%(name)s.%(funcName)s(): %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S'
        }
    },
    'handlers': {
        'mail_admins': {
            'level': 'ERROR',
            'filters': ['require_debug_false'],
            'class': 'django.utils.log.AdminEmailHandler'
        },
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'debug'
        },
        'file': {
            'level': 'DEBUG',
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': 'kive.log',
            'formatter': 'debug',
            'maxBytes': 1024*1024*15,  # 15MB
            'backupCount': 10
        }
    },
    'root': {
        # This is the default logger.
        'handlers': ['console', 'file'],
        'level': 'WARN'
    },
    'loggers': {
        # Change the logging level for an individual logger.
        # 'archive.tests': {
        #      'level': 'DEBUG'
        # },
        'django.request': {
            'handlers': ['mail_admins'],
            'level': 'ERROR',
            'propagate': True,
        },
        "fleet.Manager": {
            "level": "INFO",
        },
        "fleet.Worker": {
            "level": "INFO",
        }
    }
}

# The polling interval that the manager of the fleet uses between queries to the database.
FLEET_POLLING_INTERVAL = 30  # in seconds
FLEET_PURGING_INTERVAL = 3600  # in seconds

# The time interval the worker uses between polling for progress, in seconds.
# Shorter sleep makes worker more responsive, generates more load when idle.
SLEEP_SECONDS = 0.1


TEST_RUNNER = 'django.test.runner.DiscoverRunner'

LOGIN_URL = "login"
LOGIN_REDIRECT_URL = "home"

# Sandbox configuration

# The path to the directory that sandboxes run in.  If this is not an absolute path,
# sandboxes will run in ${MEDIA_ROOT}/${SANDBOX_PATH}.  Otherwise it will run in
# exactly this path.
SANDBOX_PATH = "Sandboxes"

# Here you specify the time that sandboxes should exist after finishing
# before being automatically purged. (They may still be manually purged
# sooner than this.)  These quantities get added up.
SANDBOX_PURGE_DAYS = 1
SANDBOX_PURGE_HOURS = 0
SANDBOX_PURGE_MINUTES = 0

# Keep this many of the most recent Sandboxes for any PipelineFamily.
SANDBOX_KEEP_RECENT = 10

# When to start purging old output datasets
DATASET_MAX_STORAGE = 5 << 40  # TB
# When to stop purging
DATASET_TARGET_STORAGE = 2 << 40  # TB

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
KIVE_PROCESSING_GROUP = "kiveprocessing"

# Number of rows to display on the View Dataset page.
DATASET_DISPLAY_MAX = 100
