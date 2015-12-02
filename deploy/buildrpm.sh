#!/bin/sh

GIT_VERSION="$(git rev-list HEAD -n 1)"
EXTENDED_VERSION="$(git log -n 1 --pretty=format:'%h (%ai)')"
BRANCH="$(git name-rev --name-only HEAD)"
BRANCH_FOR_RPM="$(echo $BRANCH|sed 's/-/_/g')"
PACKAGER="$(git config user.name) <$(git config user.email)>"
LAST_COMMIT_DATETIME="$(git log -n 1 --format='%ci' | awk '{ print $1, $2 }' | sed 's/[ :]//g;s/-//g')"
CURRENT_DATETIME=`date +'%Y%m%d%H%M%S'`

ROOT=`pwd`

RPM_TOPDIR="$ROOT/rpm"

mkdir -p ${RPM_TOPDIR}/{BUILD,RPMS,SOURCES,SRPMS,SPECS,BUILDROOT}
mkdir -p ${RPM_TOPDIR}/RPMS/{i386,i586,i686,noarch}

VERSION="$(echo $2|awk -F "-" '{print $1}')"
RELEASE="$(echo $2|awk -F "-" '{print $2}')"
if [ "$RELEASE" = "" ]; then
    RELEASE="1"
fi

rpmbuild -ba --clean $1 \
    --define "version $VERSION" \
    --define "release $RELEASE" \
    --define "packager ${PACKAGER}" \
    --define "_topdir $RPM_TOPDIR" \
    --define "_tmppath $RPM_TOPDIR/tmp" \
    --define "_sourcedir $ROOT" \
    --define "_specdir $RPM_TOPDIR/SPECS/" \
    --define "_builddir $RPM_TOPDIR/BUILD" \
    --define "_buildrootdir $RPM_TOPDIR/BUILDROOT" \
    --define "_rpmdir $RPM_TOPDIR/RPMS" \
    --define "_srcrpmdir $RPM_TOPDIR/SRPMS"
