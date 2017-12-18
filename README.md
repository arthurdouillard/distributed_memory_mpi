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

The distributed memory only works for storing `int` and `list[int]`.

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

**Initialization**:

```
mem = dm.init_memory(max_per_slave=10)
# max_per_slave is available memory per hosts
```

Note: *Your application MUST call `mem.quit()` method at the end in order to exit
gracefully.*

**Creating a variable**:

```
var_int = mem.add(42)
var_list = mem.add([1, 2, 3])
```

**Reading a variable**:

```
value_int = mem.read(var_int)
value_list = mem.read(var_list)
```

**Modifying a variable**:

Note: *A boolean is returned to inform whether the modification has taken place.*

```
bool_int = mem.modify(var_int, 1337)
bool_list = mem.modify(var_list, 42, index=0)
```

**Reducing a list**:

```
sum_list = mem.reduce(var_list, lambda x, y: x + y, 0)
```

**Mapping a list**:

Note: *The mapping may not be finished when the `mem.map` method returns.*

```
mem.map(var_list, lambda x: x ** 2)
```

**Filtering a list**:

```
mem.filter(var_list, lambda x: x % == 0)
```

**Freeing a variable**:

```
mem.free(var_int)
mem.free(var_list)
```

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
mpiexec -hostfile hostfile -n <nb_hosts> tests.py
```

## Demo

To start the demo:

```
mpiexec -hostfile -n <nb_hosts> demo.py --size <list_size> --verbose
```
