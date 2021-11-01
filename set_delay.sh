tc qdisc add dev eth0 root handle 1: prio
tc qdisc add dev eth0 parent 1:1 handle 10: netem delay 100ms 20ms distribution normal limit 100000
tc filter add dev eth0 protocol ip parent 1: prio 1 u32 match ip dst 172.24.81.136 flowid 1:1
tc qdisc add dev eth0 parent 1:2 handle 20: netem delay 100ms 20ms distribution normal limit 100000
tc filter add dev eth0 protocol ip parent 1: prio 1 u32 match ip dst 172.24.81.137 flowid 1:2
tc qdisc add dev eth0 parent 1:3 handle 30: netem delay 100ms 20ms distribution normal limit 100000
tc filter add dev eth0 protocol ip parent 1: prio 1 u32 match ip dst IP_WITHIN_CLUSTER flowid 1:3
tc qdisc add dev eth0 parent 1:4 handle 40: netem delay 100ms 20ms distribution normal limit 100000
tc filter add dev eth0 protocol ip parent 1: prio 1 u32 match ip dst IP_WITHIN_CLUSTER flowid 1:4