## ORAQUERY

* Collect data by consistent connections to oracle dbs.
* add new collections just by config with sql. 
* need a prerequisite: oracle instant client.

### install and config oracle instant client:
* installation (please choose the version by yourself. The following just show a example)
  ```bash
  wget https://download.oracle.com/otn_software/linux/instantclient/19600/oracle-instantclient19.6-basic-19.6.0.0.0-1.x86_64.rpm
  rpm -iUh oracle-instantclient19.6-basic-19.6.0.0.0-1.x86_64.rpm
* config tnsnames.ora
  ```bash
  ## create monitor user in db
  create user umsp identified by umsp;
  grant create session to umsp;
  grant select any dictionary to umsp;
  vim $(cat /etc/ld.so.conf.d/oracle-instantclient.conf)/network/admin/tnsnames.ora
### alread-configed collecting metrics:
* nonrac_dbtime
* rac_dbtime
* nonrac_performance
* rac_performance
* nonrac_topevent5
* rac_topevent5
* tablespace
* nonrac_topsql5
* rac_topsql5
### get all detailed configuration by command:
```bash
./telegraf --usage oraquery