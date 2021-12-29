tc qdisc add dev eth0 root handle 1: prio
tc qdisc add dev eth0 parent 1:1 handle 10: netem delay 80ms 60ms distribution normal limit 100000 loss 10%
tc filter add dev eth0 protocol ip parent 1: prio 1 u32 match ip dst 172.24.81.136 flowid 1:1
tc filter add dev eth0 protocol ip parent 1: prio 1 u32 match ip dst 172.24.81.137 flowid 1:1