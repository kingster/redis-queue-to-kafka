#!/usr/bin/env bash
set -ex

BUILD_ROOT=$(mktemp -d)
VERSION=$(date +%s)
cp -r debian/* $BUILD_ROOT/

mkdir -p $BUILD_ROOT/usr/local/bin/

pushd ../ > /dev/null
make clean
make
popd > /dev/null

cp ../strowger-event-relayer  $BUILD_ROOT/usr/local/bin/strowger-event-relayer

sed -i "s/_VERSION_/$VERSION/g" $BUILD_ROOT/DEBIAN/control

rm -f strowger-event-relayer.deb
dpkg-deb --build $BUILD_ROOT strowger-event-relayer.deb

rm -rf $BUILD_ROOT