for ((i=1;i<=200;i++))
do
  rados -p ecpool rm zfy_test_$i
done
for ((i=1;i<=200;i++))
do
  rados -p ecpool get zfy_test_$i zfy_test_delete_$i >> read_delete.out
done