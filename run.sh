#!/usr/bin/env bash

DEVICE=/dev/nvme0n1

rm -rf btrfs ext2 ext4 ext4-no-journal f2fs xfs

yes | mkfs.ext2 $DEVICE
mkdir output
target/release/fsbench -d $DEVICE > output/benchmark.out
mv output/ ext2

yes | mkfs.ext4 $DEVICE
mkdir output
target/release/fsbench -d $DEVICE > output/benchmark.out
mv output/ ext4

yes | mkfs.ext4 $DEVICE
tune2fs -o journal_data_writeback $DEVICE
tune2fs -O ^has_journal $DEVICE
yes | e2fsck -f $DEVICE
mkdir output
target/release/fsbench -d $DEVICE > output/benchmark.out
mv output/ ext4-no-journal

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
