#!/usr/bin/env bash

DEVICE=/dev/nvme0n1

rm -rf btrfs ext2 ext4 f2fs xfs

yes | mkfs.ext2 $DEVICE
mkdir output
target/release/fsbench -d $DEVICE > output/benchmark.out
mv output/ ext2
cd ext2
blkparse createfiles > createfiles.blkparse.out
blkparse createfiles_sync > createfiles_sync.blkparse.out
blkparse createfiles_eachsync > createfiles_eachsync.blkparse.out
blkparse deletefiles > deletefiles.blkparse.out
blkparse listdir > listdir.blkparse.out
blkparse renamefiles > renamefiles.blkparse.out
cd ..

yes | mkfs.ext4 -E lazy_itable_init=0,lazy_journal_init=0 $DEVICE
mkdir output
target/release/fsbench -d $DEVICE > output/benchmark.out
mv output/ ext4
cd ext4
blkparse createfiles > createfiles.blkparse.out
blkparse createfiles_sync > createfiles_sync.blkparse.out
blkparse createfiles_eachsync > createfiles_eachsync.out
blkparse deletefiles > deletefiles.blkparse.out
blkparse listdir > listdir.blkparse.out
blkparse renamefiles > renamefiles.blkparse.out
cd ..

mkfs.btrfs -f $DEVICE
mkdir output
target/release/fsbench -d $DEVICE > output/benchmark.out
mv output/ btrfs
cd btrfs
blkparse createfiles > createfiles.blkparse.out
blkparse createfiles_sync > createfiles_sync.blkparse.out
blkparse createfiles_eachsync > createfiles_eachsync.blkparse.out
blkparse deletefiles > deletefiles.blkparse.out
blkparse listdir > listdir.blkparse.out
blkparse renamefiles > renamefiles.blkparse.out
cd ..

mkfs.f2fs -f $DEVICE
mkdir output
target/release/fsbench -d $DEVICE > output/benchmark.out
mv output/ f2fs
cd f2fs
blkparse createfiles > createfiles.blkparse.out
blkparse createfiles_sync > createfiles_sync.blkparse.out
blkparse createfiles_eachsync > createfiles_eachsync.blkparse.out
blkparse deletefiles > deletefiles.blkparse.out
blkparse listdir > listdir.blkparse.out
blkparse renamefiles > renamefiles.blkparse.out
cd ..

mkfs.xfs -f $DEVICE
mkdir output
target/release/fsbench -d $DEVICE > output/benchmark.out
mv output/ xfs
cd xfs
blkparse createfiles > createfiles.blkparse.out
blkparse createfiles_sync > createfiles_sync.blkparse.out
blkparse createfiles_eachsync > createfiles_eachsync.blkparse.out
blkparse deletefiles > deletefiles.blkparse.out
blkparse listdir > listdir.blkparse.out
blkparse renamefiles > renamefiles.blkparse.out
cd ..
