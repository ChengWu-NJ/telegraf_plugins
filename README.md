# telegraf_plugins

> greatdolphin@gmail.com

> I developed several input plugins of github.com/influxdata/telegraf in 2020, now archived here. New ideas and bug reporting are welcome.

> The framework has been modified little bit:
* In input.go, added an interface named ServiceInputWithContext.
* In agent/agent.go, modified functions startServiceInputs and stopServiceInputs to deal with the added ServiceInputWithContext interface.

> Those plugins could be classified into 2 kinds:
* modify original counter plugins to add speed rates to those counters by calculating the difference of interval.
* create new plugins to provide developing frameworks in conditions of resource pools, such as querying oracle DBs or running bash scripts.

## DiskIO2 Input Plugin
DiskIO2 based on DiskIO input plugin, and added speed rate of reading and writing and aggregations of those rates.

## InfiniBand2 Input Plugin
This plugin gathers statistics for all InfiniBand devices and ports on the system directly from `/sys/class/infiniband`, and calculates speed rates or sending and receiving. 

## Net2 Input Plugin
Net2 based on net input plugin, and added speed rate of sending and receiving and aggregations of those rates.

## BashPool
A service to provide a pool of shells of linux bash to run script in designed linux user account, and relay stdout to output plugin
### Why write this plugin other than using the original "exec" plugin
* "exec" creates a sh process and releases it each interval. When the script needs su to another user to run, and the environment preparation of the su-ed user is complicated, it will take a long time.
* BashPool adopts a strategy like a database pool. It starts bash shell with specific user environment, and recept script sent by defined items, and run and return output.


## ORAQUERY
* Collect data by consistent connections to oracle dbs.
* add new collections just by config with sql. 
* need a prerequisite: oracle instant client.