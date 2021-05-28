# Instructions

## Generate protobuf files
```bash
make proto
```

## Unit Test
```bash
make test
```

## Benchmark Test
```bash
# benchmark all
make bench
# get profile
go test -benchmem -bench=".*Evict" -cpuprofile=cpu.profile -memprofile=mem.profile ./...
```
