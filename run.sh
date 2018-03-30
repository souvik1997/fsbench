#!/usr/bin/env bash

DEVICE=/dev/nvme0n1

rm -rf btrfs ext2 ext4 f2fs xfs

yes | mkfs.ext2 $DEVICE
mkdir output
target/release/fsbench -d $DEVICE > output/benchmark.out
mv output/ ext2

yes | mkfs.ext4 -E lazy_itable_init=0,lazy_journal_init=0 $DEVICE
mkdir output
target/release/fsbench -d $DEVICE > output/benchmark.out
mv output/ ext4

mkfs.btrfs -f $DEVICE
mkdir output
target/release/fsbench -d $DEVICE > output/benchmark.out
mv output/ btrfs

mkfs.f2fs -f $DEVICE
mkdir output
target/release/fsbench -d $DEVICE > output/benchmark.out
mv output/ f2fs

mkfs.xfs -f $DEVICE
mkdir output
target/release/fsbench -d $DEVICE > output/benchmark.out
mv output/ xfs
