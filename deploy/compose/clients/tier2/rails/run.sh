#!/bin/sh
# Rails 8.0.3's Active Record tests on the mysql2 adapter (tier 2, chapter 28),
# inside ruby:3.4-bookworm. The test config logs in as T2_USER/T2_PASSWORD
# at MYSQL_HOST:MYSQL_PORT; the two test databases are made here, since
# rake's db:mysql tasks need the mysql client and a passwordless root.
set -eu
git clone -q --depth 1 --branch v8.0.3 https://github.com/rails/rails.git /r
cd /r
bundle config set --local without 'rubocop mdl doc job cable storage'
bundle install -j2 >/tmp/bundle.log 2>&1 || { tail -30 /tmp/bundle.log; exit 1; }
cd activerecord
sed -e "s/^\( *\)username: rails$/\1username: \"$T2_USER\"\n\1password: \"$T2_PASSWORD\"/" \
  test/config.example.yml > test/config.yml
bundle exec ruby -e '
  require "mysql2"
  c = Mysql2::Client.new(host: ENV["MYSQL_HOST"], port: Integer(ENV["MYSQL_PORT"]),
                         username: ENV["T2_USER"], password: ENV["T2_PASSWORD"])
  %w[activerecord_unittest activerecord_unittest2].each do |d|
    c.query("DROP DATABASE IF EXISTS #{d}")
    c.query("CREATE DATABASE #{d} DEFAULT CHARACTER SET utf8mb4")
  end'
TESTOPTS=-v bundle exec rake test:mysql2 2>&1 || true
