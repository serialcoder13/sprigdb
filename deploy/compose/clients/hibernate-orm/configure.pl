#!/usr/bin/perl
# Points Hibernate ORM's `mysql_ci` database profile at one endpoint
# (deploy/compose/clients/run.sh hibernate-orm). Run in the root of a copy
# of the hibernate-orm tree:
#
#   perl configure.pl <host:port> <user> <password>
#
# The profile lives in a Gradle script (local-build-plugins/.../
# local.databases.gradle in 6.x, gradle/databases.gradle before): its URL
# uses dbHost (localhost by default) and the login hibernate_orm_test. Only
# that block changes: the URL's host part, jdbc.user and jdbc.pass. Fails
# when no file defines the profile, so an upstream rename is noticed.
use strict;
use warnings;
use File::Find;

my ($hostport, $user, $pass) = @ARGV;
die "usage: configure.pl <host:port> <user> <password>\n" unless defined $pass;
for ($user, $pass) { die "quote in a login value\n" if /'/; }

my @files;
find(sub {
    return if $File::Find::dir =~ m{/(\.git|target|build|\.gradle)(/|$)};
    push @files, $File::Find::name if /\.gradle(\.kts)?$/;
}, '.');

my $done = 0;
for my $f (@files) {
    open(my $in, '<', $f) or die "$f: $!";
    local $/;
    my $text = <$in>;
    close $in;
    next unless $text =~ /\bmysql_ci\s*:\s*\[/;
    my $n = ($text =~ s{(\bmysql_ci\s*:\s*\[)(.*?)(\n\s*\])}{
        my ($head, $body, $tail) = ($1, $2, $3);
        $body =~ s{('jdbc\.user'\s*:\s*)'[^']*'}{$1'$user'};
        $body =~ s{('jdbc\.pass'\s*:\s*)'[^']*'}{$1'$pass'};
        $body =~ s{'jdbc:mysql://'\s*\+\s*dbHost\s*\+\s*'}{'jdbc:mysql://$hostport};
        $body =~ s{jdbc:mysql://localhost(:\d+)?}{jdbc:mysql://$hostport};
        print "mysql_ci in $f:\n$body\n";
        "$head$body$tail";
    }se);
    next unless $n;
    open(my $out, '>', $f) or die "$f: $!";
    print $out $text;
    close $out;
    $done++;
}
die "configure.pl: no Gradle script defines the mysql_ci profile\n" unless $done;
