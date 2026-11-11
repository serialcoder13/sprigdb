# MySQL's mysql-test-run.pl and mysqltest for CI's heavy-suite job
# (deploy/compose/clients/run.sh mysql-test-run; chapter 46 step 10). The
# test package (mysql-community-test, GPL, a test tool: nothing of it ships
# with SprigDB) of the same release as the reference server, on top of the
# official image, which already has the matching mysql client. The RPM is
# checked against MySQL's signing key that the image installed for its own
# packages, then installed without its dependency on the full server (the
# tests run against an external server, --extern). Built on CI runners only.
ARG VERSION=8.4.11
FROM mysql:${VERSION}
ARG VERSION
RUN set -eux; \
    microdnf install -y perl perl-JSON perl-Time-HiRes perl-Data-Dumper perl-Env perl-Digest-MD5 \
        perl-File-Temp perl-Getopt-Long perl-Sys-Hostname perl-IPC-Cmd findutils diffutils; \
    microdnf clean all; \
    cd /tmp; \
    rpm=mysql-community-test-${VERSION}-1.el9.$(uname -m).rpm; \
    curl -fsSLO "https://repo.mysql.com/yum/mysql-8.4-community/el/9/$(uname -m)/$rpm"; \
    for k in /etc/pki/rpm-gpg/*; do rpm --import "$k"; done; \
    rpm -K "$rpm" | grep -qi 'digests signatures ok'; \
    rpm -i --nodeps "$rpm"; \
    rm -f "$rpm"; \
    test -x /usr/bin/mysqltest; \
    test -f "$(dirname "$(rpm -ql mysql-community-test | grep '/mysql-test-run.pl$' | head -n 1)")/mysql-test-run.pl"
