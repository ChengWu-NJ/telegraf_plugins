# InfiniBand2 Input Plugin

This plugin gathers statistics for all InfiniBand devices and ports on the
system directly from `/sys/class/infiniband`, and calculates speed rates or sending and receiving. 

**Supported Platforms**: Linux

### Configuration

```toml
[[inputs.infiniband2]]
  # no configuration
```

### Metrics

- infiniband2
  - tags:
    - cname
    - port
    - phys_state
    - state
  - fields:
    - rate_gb
    - send_bytes
    - recv_bytes

### Example Output

```
infiniband,caname=mlx4_0,host=gfs03,phys_state=LinkUp,port=1,serial_number=DJY5232,state=ACTIVE,user=root rate_gb=40i,send_bytes=12340i,recv_bytes=2340i 1590720053000000000

```
