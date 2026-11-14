# Django's own test suite against a MySQL endpoint (tier 2):
# PyMySQL stands in for mysqlclient (no compiler in the image).
import os

import pymysql

pymysql.install_as_MySQLdb()


def db(name):
    return {
        "ENGINE": "django.db.backends.mysql",
        "HOST": os.environ["T2_HOST"],
        "PORT": os.environ["T2_PORT"],
        "USER": os.environ["T2_USER"],
        "PASSWORD": os.environ["T2_PASSWORD"],
        "NAME": name,
        "TEST": {"NAME": "test_" + name, "CHARSET": "utf8mb4"},
        "OPTIONS": {"charset": "utf8mb4"},
    }


DATABASES = {"default": db("tiertwo_django"), "other": db("tiertwo_django_other")}
SECRET_KEY = "django_tests_secret_key"
PASSWORD_HASHERS = ["django.contrib.auth.hashers.MD5PasswordHasher"]
DEFAULT_AUTO_FIELD = "django.db.models.AutoField"
USE_TZ = False
