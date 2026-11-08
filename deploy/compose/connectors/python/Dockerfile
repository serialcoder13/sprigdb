# PyMySQL and mysqlclient (MySQLdb, built against Debian's libmariadb).
# cryptography lets PyMySQL do caching_sha2_password's RSA exchange over a
# plaintext connection.
FROM python:3.12-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends gcc libc6-dev pkg-config default-libmysqlclient-dev \
 && rm -rf /var/lib/apt/lists/*
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY check.py /app/check.py
ENTRYPOINT ["python3", "-I", "/app/check.py"]
