## Overview

BPar - Yet another worker pool..

Extended from TUlib (Klarna's standard Erlang library)

## Authors

* [Björn Jensen-Urstad](mailto:bjorn.jensen.urstad@gmail.com)

## Installation
mrnasty@gimli:~/bpar$ make

mrnasty@gimli:~/bpar$ make test

## Example

Please look at the tests.

```erlang
{ok, Pid} = bpar:start_link([{mod, meh}, {args, [foo]}, {size, 16}]),
%% run task synchronously
bpar:run(Pid, task1),
%% run task asynchronously
bpar:run_async(Pid, task2),
%% run task asynchronously (but block until a worker is ready to pick it up)
bpar:run_async_wait(Pid, task3),
%% flush work (block until workers are idle, will also cause pool to stop accepting tasks).
bpar:flush(Pid),
```

## Manifest
* src/
    * bpar.erl              -- API and implementation
* test/
    * bpar_proper_test.erl  -- Simple Proper test
    * bpar_test_worker.erl  -- Test worker callback module
