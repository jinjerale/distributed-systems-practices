## Ref
- [github](git://g.csail.mit.edu/6.5840-golabs-2024 6.5840)
- [doc](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html)
## MapReduce

### Setup
```
cd src/main
go build -buildmode=plugin ../mrapps/wc.go
rm mr-out
go run mrsequential.go wc.go wc.so pg*.txt
more mr-out-0
```

### Test
Manually:
```
rm mr-out*
go run mrcoordinator.go pg-*.txt
## in other window
go run mrworker.go wc.so
## once done
cat mr-out-* | sort | more
```
Test script:
```
bash test-mr.sh
```

## Key/Value Server
```
cd src/kvsrv
go test
```