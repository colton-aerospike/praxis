# praxis
Learning Aerospike golang client. Quick test tool to run various workloads.

## Usage
```bash
praxis -U <user> -P <password> -h <hostname> -sa -n test -s test -k 10000 -q -iB color -iV blue -chan 50 -sT 5s
```

```bash
% ./praxis --help
Usage of ./praxis:
  -A string
        AuthMode for Aerospike (needs to be implemented, hardcoded internal currently) (default "internal")
  -P string
        ClientPolicy password for authentication (default "data")
  -U string
        ClientPolicy user for authentication (default "data")
  -b string
        Bins to set encapsulated in quotes separated by commas 'bin1:hello,bin2:24,color:green'
  -chan int
        Size of channel for goroutines (default 500)
  -h string
        Remote host (default "172.17.0.2")
  -iB string
        Index Bin name for SI query
  -iV string
        Index Bin value for SI query
  -k int
        Upper primary key range for single read/write. Default: 1000 (default 1000)
  -n string
        Namespace (default "bar")
  -p int
        Remote port (default 3000)
  -q    Run short SI query; default false
  -s string
        Set name (default "myset")
  -sT duration
        Time to sleep in between executions of workload (default 1s)
  -sa
        Enable --alternate-services; default false
```

