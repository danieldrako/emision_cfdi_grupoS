from decouple import config

vertica_host = config('VERTICA_HOST')
vertica_port = config('VERTICA_PORT', default=5433, cast=int)
vertica_user = config('VERTICA_USER')
vertica_password = config('VERTICA_PASSWORD')