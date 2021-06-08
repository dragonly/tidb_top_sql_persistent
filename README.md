# Instructions

## Monkey testing server
The monkey testing server is used as a mock server, which can drop TCP packets on demand.
It can provide a convenient way for manual testing the conditions when network connection to the agent is not working.

```bash
$ go run main.go monkey  # starts the monkey testing server
$ drop 10  # tell the monkey server to drop TCP traffic in the next 10s
```

## Generate protobuf files
```bash
$ make proto
```

## Unit Test
```bash
$ make test
```

## Benchmark Test
```bash
# benchmark all
$ make bench
# get profile
$ go test -benchmem -bench=".*Evict" -cpuprofile=cpu.profile -memprofile=mem.profile ./...
```
