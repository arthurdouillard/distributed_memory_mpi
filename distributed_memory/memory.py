import collections
import logging
import heapq
import random
import time

from mpi4py import MPI
import numpy as np
import mmh3
import dill

from .tags import Tags
from .collector import Collector
from .logger import log


class Variable:
    def __init__(self, var_names, var_type):
        self.var_names = var_names
        self.var_type = var_type


    def __bool__(self):
        return len(self.var_names) > 0


def init_memory(*, max_per_slave):
    """Entry point to the distributed memory.

    max_per_slave -- Maximum amount of elements stored by a slave.

    Returns a `Memory` object. Every variables manipulation are made throught
    this interface. No need to handle the current processus' rank.

    Ex:
    >>> mem = init_memory(max_per_slave=100)
    >>> var1 = mem.add([1, 2, 3])
    >>> mem.map(var1, lambda x: x + 1)
    >>> mem.read(var1)
    [2, 3, 4]
    """
    if MPI.COMM_WORLD.Get_rank() == 0:
        return Memory(max_per_slave=max_per_slave)

    collector = Collector()
    collector.run()


class Memory:
    """Interface to the distributed memory and Master in the centralized topology."""
    def __init__(self, *, max_per_slave):
        """Init a `Memory`.

        max_per_slave -- Maximum amount of elements stored by a slave.

        The user should initialize himself the `Memory`, the function
        `init_memory` should be used instead.
        """
        self.log = logging.getLogger(' Master').info
        self.log('Starting Memory...')

        self.comm = MPI.COMM_WORLD

        self.nb_slaves = self.comm.Get_size() - 1 # Minus Master
        self.max_per_slave = max_per_slave
        self.slaves_tracking = collections.defaultdict(int)
        self.list_tracking = dict()


    @log('Add')
    def add(self, var):
        """Add a variable @var to the distributed memory.

        var -- Variable to add to the distributed memory. Either an `int` of a
               `list` of `int`.

        Ex:
        >>> var1 = mem.add([1, 2, 3])
        >>> var2 = mem.add(42)
        """
        if not isinstance(var, int) and not isinstance(var, list):
            raise ValueError("""Expecting either an `int` or a `list`,
                                not a `{}`""".format(type(var).__name__))

        # Choosing the slaves
        var_size = 1 if isinstance(var, int) else len(var)
        selected_slaves = []
        for slave_id in range(1, self.nb_slaves+1):
            if self.slaves_tracking[slave_id] + var_size < self.max_per_slave:
                selected_slaves = [(slave_id, var_size)]
                break # Try to fit the whole variable into a single slave
        else: # Place portion of the variable into several slaves
            smallest_slaves = sorted(self.slaves_tracking.items(), key=lambda x: x[1])
            for (slave_id, size) in smallest_slaves:
                remaining_size = self.max_per_slave - size

                if remaining_size == 0 and var_size > 0:
                    raise Exception("""Not enough memory! 1""")

                amount_stored = min(var_size, remaining_size)
                var_size -= amount_stored
                selected_slaves.append((slave_id, amount_stored))

                if var_size == 0:
                    break
            else:
                raise Exception("""Not enough memory! 2""")

        accumulated_amount = 0
        if var_size == 1: # Single integer
            slave_id, _ = selected_slaves[0]
            self.comm.isend(var, dest=slave_id, tag=Tags.alloc)
            self.slaves_tracking[slave_id] += 1
        else: # List
            tmp_list_tracking = []
            for slave_id, amount in selected_slaves:
                low_bound = accumulated_amount
                high_bound = accumulated_amount + amount

                tmp_list_tracking.append((low_bound, high_bound-1))

                self.comm.isend(var[low_bound:high_bound],
                                dest=slave_id, tag=Tags.alloc)

                self.slaves_tracking[slave_id] += amount
                accumulated_amount += amount

        # Gathering the id associated to the newly allocated variable
        var_names = []
        for i, (slave_id, _) in enumerate(selected_slaves):
            var_name = self.comm.recv(source=slave_id, tag=Tags.alloc)
            var_names.append(var_name)

            if isinstance(var, list):
                self.list_tracking[var_name] = tmp_list_tracking[i]

        return Variable(var_names, type(var))


    @log('Read')
    def read(self, var):
        """Read a variable @var_name from the distributed memory.

        var -- `Variable` instance

        Ex:
        >>> var1 = mem.add(42)
        >>> var2 = mem.add([1, 2, 3])
        >>> mem.read(var1)
        42
        >>> mem.read(var2)
        [1, 2, 3]
        """
        values = []
        for var_name in var.var_names:
            slave_id = Collector.get_slave_id(var_name)
            self.comm.isend(var_name, dest=slave_id, tag=Tags.read)
            var = self.comm.recv(source=slave_id, tag=Tags.read)
            if isinstance(var, int):
                return var
            else:
                values.extend(var)

        return values


    @log('Modify')
    def modify(self, var, new_value, index=None):
        """Modify an existing variable @var_name with the value @new_value.

        var       -- `Variable` instance
        new_value -- Update the @var Variable with this new value.

        Ex:
        >>> var = mem.add(42)
        >>> mem.read(var)
        42
        >>> mem.modify(var, 1337)
        True
        >>> mem.read(var)
        1337
        """
        if not isinstance(new_value, int):
            raise ValueError("""@new_value must be of type `int`
                                not {}.""".format(type(new_value).__name__))

        if var.var_type == int:
            slave_id = Collector.get_slave_id(var.var_names[0])
            self.comm.send((var.var_names[0], new_value, index, time.time()),
                           dest=slave_id, tag=Tags.modify)

            return self.comm.recv(source=slave_id, tag=Tags.modify)
        else:
            if not isinstance(index, int):
                raise ValueError("""Index must be an integer
                                    not {}.""".format(type(index).__name__))

            accumulated_index = 0
            for var_i, var_name in enumerate(var.var_names):
                low_bound, high_bound = self.list_tracking[var_name]
                if low_bound <= index <= high_bound:
                    if var_i != 0:
                        index = index - accumulated_index - 1
                    slave_id = Collector.get_slave_id(var_name)

                    self.comm.send((var_name, new_value, index, time.time()),
                                   dest=slave_id, tag=Tags.modify)

                    return self.comm.recv(source=slave_id, tag=Tags.modify)
                else:
                    accumulated_index += high_bound

        raise Exception("""Out of bounds error with index {}.""".format(index))


    @log('Free')
    def free(self, var):
        """Free an existing variable @var_name.

        var_names -- Variable id

        Ex:
        >>> var = mem.add(42)
        >>> mem.free(var)
        >>> mem.read(var)
        *error raised*
        """
        if not var:
            raise Exception("""Double free.""")

        for var_name in var.var_names:
            slave_id = Collector.get_slave_id(var_name)
            self.comm.isend(var_name, dest=slave_id, tag=Tags.free)

            nb_freed = self.comm.recv(source=slave_id, tag=Tags.free)
            # Remove any info related to @var_name while send is processing.
            self.slaves_tracking[slave_id] -= nb_freed
            self.list_tracking.pop(var_name, None)

        var.var_names = []


    @log('Map')
    def map(self, var, fun):
        """Map in-place the function @fun to the variables @var_names.

        var -- `Variable` instance
        fun -- Function applied to the mapping. Takes a single input.

        Ex:
        >>> var = mem.add([1, 2, 3])
        >>> mem.map(var, lambda x: x + 1)
        >>> mem.read(var)
        [2, 3, 4]
        """
        for var_name in var.var_names:
            slave_id = Collector.get_slave_id(var_name)
            msg = (var_name, dill.dumps(fun))
            self.comm.isend(msg, dest=slave_id, tag=Tags.map)


    @log('Filter')
    def filter(self, var, fun):
        """Filter in-place the variables @var_names according to function @fun.

        Returns `None` if the `filter` eliminate everything (`filter` with a
        `False` function is equivalent to `free`). Otherwise returns the new
        associated variable names.

        var -- `Variable` instance
        fun -- Function applied to the filtering. Takes a single input and
               returns a boolean.

        Ex:
        >>> var = mem.add([1, 2, 3])
        >>> var = mem.filter(var, lambda x: x % 2 == 0)
        >>> mem.read(var)
        [2]
        """
        to_remove = []
        for var_name in var.var_names:
            slave_id = Collector.get_slave_id(var_name)
            msg = (var_name, dill.dumps(fun))
            self.comm.isend(msg, dest=slave_id, tag=Tags.filter)

            diff_len, presence = self.comm.recv(source=slave_id, tag=Tags.filter)
            self.slaves_tracking[slave_id] -= diff_len

            if not presence:
                to_remove.append(var_name)

        for var_name in to_remove:
            var.var_names.remove(var_name)


    @log('Reduce')
    def reduce(self, var, fun, initial_value):
        """Reduce the variables @var_names with the function @fun.

        var_names     -- Variable id
        fun           -- Function applied to reduce. Must take two input args.
        initial_value -- Initial value for the reduce function.

        Ex:
        >>> var = mem.add([1, 2, 3])
        >>> mem.reduce(var, lambda x, y: x + y, 100)
        106
        """
        slave_id_first = Collector.get_slave_id(var.var_names[0])
        slave_id_last = Collector.get_slave_id(var.var_names[-1])

        msg = (var.var_names, dill.dumps(fun), initial_value)

        self.comm.isend(msg, dest=slave_id_first, tag=Tags.reduce)
        val = self.comm.recv(source=slave_id_last, tag=Tags.reduce)
        return val


    @log('Quit')
    def quit(self):
        """Close each slave then itself.

        This function MUST be called at the end of the program in order to exit
        gracefully.
        """
        pending = []
        for slave_id in range(1, self.nb_slaves+1):
            req = self.comm.isend(0, dest=slave_id, tag=Tags.quit)
            pending.append(req)

        for req in pending:
            req.wait()

        exit(0)
