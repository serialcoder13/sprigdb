# PHP PDO_MySQL and mysqli, both on mysqlnd (bundled with PHP; the PHP
# version pins them).
FROM php:8.4.14-cli
RUN docker-php-ext-install pdo_mysql mysqli
COPY check.php /app/check.php
ENTRYPOINT ["php", "-d", "memory_limit=512M", "/app/check.php"]
