# Distributed memory with MPI in Python

This project's goal is to produce a distributed memory. This project is made in
Python and will use `mpi4py`, the Python bindings for `MPI` for message passing
between the different machines hosting the distributed memory.

At least two hosts are needed.

To launch your python script using the distributed memory (`-n` is the number
of emulated hosts):

```
mpiexec -hostfile hostfile -n 5 demo.py
```

## Dependencies

- Install [MPI](https://www.open-mpi.org/nightly/v3.0.x/)
- Install the Python requirements:

```
pip3 install -r requirements.txt
```

## The API

```
import distributed_memory as dm
```

- Initialization:


## Examples

```
import distributed_memory as dm

mem = mem.init_memory(max_per_slave=10)

var = mem.add([1, 2, 3])
print(mem.read(var))

mem.map(var, lambda x: x + 1)
print(mem.read(var))

sum_result = mem.reduce(var, lambda x, y: x + y, 0)
print(sum_result)

mem.filter(var, lambda x: x % 2 == 0)
print(mem.read(var))
```

## Tests

To launch the tests:

```
mpiexec -hostfile hostfile -n 5 tests.py
```