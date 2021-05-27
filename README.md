# Instructions

## Unit Test
```bash
go test -v ./...
```

## Benchmark Test
```bash
# benchmark all
go test -benchmem -bench=".*" ./...
# get profile
go test -benchmem -bench=".*Evict" -cpuprofile=cpu.profile -memprofile=mem.profile ./...
```
