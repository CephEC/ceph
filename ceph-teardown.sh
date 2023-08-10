#! /bin/sh
#
# ceph-teardown.sh
# Copyright (C) 2023 root <root@ce2>
#
# Distributed under terms of the MIT license.
#
block_device=`lsblk | grep -w -B1 "ceph" | awk '{print $1}' | sed '1~2!d'`
dmsetup=`dmsetup status | grep ceph | awk -F: '{print $1}'`
for status in $dmsetup
do
	dmsetup remove $status
done
for device in $block_device
do
	wipefs -a /dev/$device
	sgdisk --zap-all /dev/$device
	dd if=/dev/zero of=/dev/$device bs=1M count=100 oflag=direct,dsync
done


