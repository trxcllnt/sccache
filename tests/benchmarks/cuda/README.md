# CUDA benchmark

Build the `simpleP2P.cu` example from the CUDA Samples with sccache.

* `--iterations N` determines the total number of times to compile `src/simpleP2P.cu`
* `--jobs J` determines the number of concurrent compilations to run
* `--threads T` determines the number of device architectures to build concurrently (per compilation)

For example, the following command compiles `src/simpleP2P.cu` 200 times total, 100 at a time, and up to 15 device architectures concurrently per compilation:

```shell
./tests/benchmarks/cuda/test.sh --clear-cache --build-type release --iterations 200 --jobs 100 --threads 15
```

If using this script to validate `sccache-dist` performance, be aware that high levels of concurrency can lead to client and server errors due to exceeding the default `nofile` ulimits (e.g. 1024 on Ubuntu). You will need to increase the `nofile` ulimit of your current shell before starting the scheduler, servers, and client (e.g. `ulimit -n $(ulimit -Hn)`).
