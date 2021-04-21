## BashPool

A service to provide a pool of shells of linux bash to run scripts in designed linux user accounts, and relay stdout to output plugin

### Why write this plugin other than using the original "exec" plugin
* "exec" creates a sh process and releases it each interval. When the script needs su to another user to run, and the environment preparation of the su-ed user is complicated, it will take a long time.
* BashPool adopts a strategy like a database pool. It starts bash shell with specific user environment, and recept script sent by defined items, and run and return output.

### config
There are 2 kinds of object definitions for this plugin:
* user_shell: a unique id, start bash by which os user.
* cmd_define: a unique id, run on which user_shell, the cmd line content, gathering interval in seconds.

