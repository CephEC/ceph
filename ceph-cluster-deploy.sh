#! /bin/sh
#
# ceph-cluster-deploy.sh
# Copyright (C) 2023 root <root@ce3>
#
# Distributed under terms of the MIT license.
#
#version=$1
action=$1
cd /root/ceph-cluster
if [ "$action" = "teardown" ] || [ "$action" = "all" ]; then
	#set +m kill -9 $(ps -ef | grep ceph | awk '{print $2}')
	ssh root@ce3 "pkill ceph"
	ssh root@ce2 "pkill ceph"
	ssh root@ce4 "pkill ceph"
	ceph-deploy purge ce3
	ceph-deploy purge ce2
	ceph-deploy purge ce4
	ceph-deploy purgedata ce2
	ceph-deploy purgedata ce3
	ceph-deploy purgedata ce4
	bash /root/ceph-teardown.sh
	ssh root@ce2 "bash /root/ceph-teardown.sh"
	ssh root@ce2 "bash /root/tmp_teardown_ceph.sh"
	bash /root/tmp_teardown_ceph.sh
	ssh root@ce4 "bash /root/ceph-teardown.sh"
	ssh root@ce4 "bash /root/tmp_teardown_ceph.sh"
fi
if [ "$action" = "deploy" ] || [ "$action" = "all" ]; then
	dnf makecache
	echo y | dnf install ceph
	echo y | dnf install ceph-volume
	ssh root@ce2 "dnf makecache"
	ssh root@ce2 "echo y | dnf install ceph"
	ssh root@ce2 "echo y | dnf install ceph-volume"
	ssh root@ce4 "dnf makecache"
	ssh root@ce4 "echo y | dnf install ceph"
	ssh root@ce4 "echo y | dnf install ceph-volume"

	ceph-deploy new --public-network 192.168.90.0/24 ce3
	cp ../ceph.conf.ec ceph.conf
	ceph-deploy --overwrite-conf config push ce3
	ceph-deploy --overwrite-conf config push ce2
	ceph-deploy --overwrite-conf config push ce4
	ceph-deploy mon create-initial
	cp ./ceph.client.admin.keyring /etc/ceph/
	ceph --connect-timeout=25 --cluster=ceph --name mon. --keyring=/var/lib/ceph/mon/ceph-ce3/keyring auth get-or-create client.admin osd 'allow *' mon 'allow *' mds 'allow *' > /etc/ceph/ceph.keyring
	ceph --connect-timeout=25 --cluster=ceph --name mon. --keyring=/var/lib/ceph/mon/ceph-ce3/keyring auth get-or-create client.admin osd 'allow *' mon 'allow *' mds 'allow *' > /etc/ceph/keyring
	ceph --connect-timeout=25 --cluster=ceph --name mon. --keyring=/var/lib/ceph/mon/ceph-ce3/keyring auth get-or-create client.admin osd 'allow *' mon 'allow *' mds 'allow *' > /etc/ceph/keyring.bin
	ceph-deploy mgr create ce2
	ceph config set mon auth_allow_insecure_global_id_reclaim false
	ceph-deploy osd create ce2 --data /dev/sdc
	ceph-deploy osd create ce2 --data /dev/sdd
	ceph-deploy osd create ce2 --data /dev/sdi
	ceph-deploy osd create ce2 --data /dev/sdj
	ceph-deploy osd create ce2 --data /dev/sdk
	ceph-deploy osd create ce2 --data /dev/sdl
	ceph-deploy osd create ce2 --data /dev/sda
	ceph-deploy osd create ce2 --data /dev/sde

	ceph-deploy osd create ce3 --data /dev/sdd
	ceph-deploy osd create ce3 --data /dev/sde
	ceph-deploy osd create ce3 --data /dev/sdf
	ceph-deploy osd create ce3 --data /dev/sdg
	ceph-deploy osd create ce3 --data /dev/sdh
	ceph-deploy osd create ce3 --data /dev/sdi
	ceph-deploy osd create ce3 --data /dev/sdj
	ceph-deploy osd create ce3 --data /dev/sdk

	ceph-deploy osd create ce4 --data /dev/sdc
	ceph-deploy osd create ce4 --data /dev/sdd
	ceph-deploy osd create ce4 --data /dev/sde
	ceph-deploy osd create ce4 --data /dev/sdf
	ceph-deploy osd create ce4 --data /dev/sdg
	ceph-deploy osd create ce4 --data /dev/sdh
	ceph-deploy osd create ce4 --data /dev/sdi
	ceph-deploy osd create ce4 --data /dev/sdj

	ceph tell osd.* injectargs --debug-osd 15/15
	scp /etc/ceph/* root@ce1:/etc/ceph

fi
