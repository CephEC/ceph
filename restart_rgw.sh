#! /bin/sh
#
# restart_rgw.sh
# Copyright (C) 2023 root <root@ce3>
#
# Distributed under terms of the MIT license.
#
cd /root/ceph-cluster

echo y | dnf install ceph-radosgw
ceph-deploy rgw create ce3
ceph osd erasure-code-profile set huge-trunk stripe_unit=128M k=4 m=2 crush-failure-domain=osd crush-root=default
ceph osd pool create default.rgw.ectest.data 128 128 erasure huge-trunk
ceph osd pool create default.rgw.ectest.index 128 128
ceph osd pool create default.rgw.ectest.non-ec 128 128
radosgw-admin zonegroup placement add --rgw-zonegroup default  --placement-id ectest
radosgw-admin zone placement modify --rgw-zone default --placement-id default-placement --data-pool default.rgw.ectest.data --index-pool default.rgw.ectest.index --data-extra-pool default.rgw.ectest.non-ec
systemctl restart ceph-radosgw@rgw.ce3.service
