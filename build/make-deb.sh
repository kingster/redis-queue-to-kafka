#!/usr/bin/env bash
set -ex

PACKAGE="strowger-relayer"

die(){
 echo $1;
 exit 1
}

function replacePlaceHolders() {
    file="$1"
    sed -i -e "s/_PACKAGE_/$PACKAGE/g" $file
    sed -i -e "s/_UID_/$PAC_UID/g" $file
    sed -i -e "s/_GID_/$PAC_GID/g" $file
    sed -i -e "s/_GROUP_/$PAC_GROUP/g" $file
    sed -i -e "s/_USER_/$PAC_USER/g" $file
}

export PAC_UID=5002
export PAC_GID=4000
export PAC_GROUP="strowger"
export PAC_USER="strowger-relayer"


BUILD_ROOT=$(mktemp -d)
VERSION=$(date +%s)
cp -r debian/* $BUILD_ROOT/

mkdir -p $BUILD_ROOT/usr/local/bin/

pushd ../ > /dev/null
make clean
make
popd > /dev/null

cp ../strowger-event-relayer  $BUILD_ROOT/usr/local/bin/strowger-event-relayer


#replacing constants
replacePlaceHolders "${BUILD_ROOT}/DEBIAN/prerm"
replacePlaceHolders "${BUILD_ROOT}/DEBIAN/postrm"
replacePlaceHolders "${BUILD_ROOT}/DEBIAN/postinst"
replacePlaceHolders "${BUILD_ROOT}/DEBIAN/control"
replacePlaceHolders "${BUILD_ROOT}/etc/systemd/system/${PACKAGE}.service"

sed -i "s/_VERSION_/$VERSION/g" $BUILD_ROOT/DEBIAN/control

rm -f strowger-event-relayer.deb
dpkg-deb --build $BUILD_ROOT strowger-event-relayer.deb

rm -rf $BUILD_ROOT