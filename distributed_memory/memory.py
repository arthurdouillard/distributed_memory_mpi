import collections
import logging
import random

from mpi4py import MPI
import numpy as np
import mmh3

from .tags import Tags
from .collector import Collector
from .clock import Clock
from .logger import log


def init_memory(*, max_per_slave):
    if MPI.COMM_WORLD.Get_rank() == 0:
        return Memory(max_per_slave=max_per_slave)

    collector = Collector()
    collector.run()


class Memory:
    def __init__(self, *, max_per_slave):
        self.log = logging.getLogger(' Master').info
        self.log('Starting Memory...')

        self.comm = MPI.COMM_WORLD

        self.clock = Clock()
        self.nb_slaves = self.comm.Get_size() - 1 # Minus Master
        self.max_per_slave = max_per_slave
        self.slaves_tracking = collections.defaultdict(int)
        self.vars_env = dict()


    @log('Add')
    def add(self, var):
        """Add a variable @var to the distributed memory."""
        if not isinstance(var, int) and not isinstance(var, list):
            raise ValueError("""Expecting either an `int` or a `list`,
                                not a `{}`""".format(type(var).__name__))
        # TODO Handles multiple variables

        # Choosing the slave - strategy to improve TODO
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
        for slave_id, amount in selected_slaves:
            self.comm.isend(var[accumulated_amount:amount+accumulated_amount],
                            dest=slave_id, tag=Tags.alloc)

            self.slaves_tracking[slave_id] += amount
            accumulated_amount += amount

        # Gathering the id associated to the newly allocated variable
        var_ids = []
        for slave_id, _ in selected_slaves:
            var_id = self.comm.recv(source=slave_id, tag=Tags.alloc)
            self.vars_env[var_id] = slave_id
            var_ids.append(var_id)

        return var_ids


    @log('Read')
    def read(self, var_names):
        """Read a variable @var_name from the distributed memory."""

        values = []

        for var_name in var_names:
            slave_id = self.vars_env[var_name]
            self.comm.isend(var_name, dest=slave_id, tag=Tags.read)
            var = self.comm.recv(source=slave_id, tag=Tags.read)
            if isinstance(var, int):
                values.append(var)
            else:
                values.extend(var)

        return values


    @log('Modify')
    def modify(self, var_name, new_value):
        """Modify an existing variable @var_name with the value @new_value."""
        slave_id = self.vars_env[var_name]
        self.comm.send((var_name, new_value), dest=slave_id, tag=Tags.modify)

        return self.comm.recv(source=slave_id, tag=Tags.modify)


    @log('Free')
    def free(self, var_name):
        """Free an existing variable @var_name."""
        slave_id = self.vars_env[var_name]
        req = self.comm.isend(var_name, dest=slave_id, tag=Tags.free)

        # Remove any info related to @var_name while send is processing.
        self.slaves_tracking[slave_id] -= 1
        del self.vars_env[var_name]
        req.wait()


    @log('Quit')
    def quit(self):
        """Close each slave then itself."""
        pending = []
        for slave_id in range(1, self.nb_slaves+1):
            req = self.comm.isend(0, dest=slave_id, tag=Tags.quit)
            pending.append(req)

        for req in pending:
            req.wait()

        exit(0)
