import logging
import operator as op
import heapq

from mpi4py import MPI
import mpi4py
import numpy as np
import dill

from .tags import Tags
from .clock import Clock
from .logger import log

class Collector:
    def __init__(self):
        self.comm = MPI.COMM_WORLD
        self.rank = self.comm.Get_rank()
        self.size = self.comm.Get_size()

        self.clock = Clock()
        self.__counter = 0
        self.__vars = dict()
        self.log = logging.getLogger(' SLAVE-{}'.format(self.rank)).debug


    @log('Running...')
    def run(self):
        while True:
            status = MPI.Status()
            msg = self.comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG,
                                 status=status)

            source = status.Get_source()
            tag = status.Get_tag()
            action = Tags.name(tag)

            if action == 'alloc':
                new_id = self.allocate_var(msg)
                self.comm.send(new_id, dest=source, tag=tag)
            elif action == 'read':
                self.comm.send(self.read_var(msg), dest=source, tag=tag)
            elif action == 'modify':
                self.comm.send(self.modify_var(msg[0], msg[1]), dest=source,
                               tag=tag)
            elif action == 'free':
                self.free_var(msg)
            elif action == 'map':
                self.map(msg[0], dill.loads(msg[1]))
            elif action == 'filter':
                self.filter(msg[0], dill.loads(msg[1]))
            elif action == 'reduce':
                msg, next_dest = self.reduce(msg[0], msg[1], msg[2])
                self.comm.send(msg, dest=next_dest, tag=Tags.reduce)
            elif action == 'quit':
                self.quit()
            else:
                raise ValueError("""Unkown tag {}:{}.""".format(tag, action))


    @log('Reducing')
    def reduce(self, var_names, fun_dump, initial_value):
        fun = dill.loads(fun_dump)
        var_name = var_names[0]
        value = self.__vars[var_name]

        if isinstance(value, int):
            initial_value = fun(initial_value, value)
        else:
            for v in value:
                initial_value = fun(initial_value, v)

        var_names = var_names[1:]
        if len(var_names) == 0:
            return initial_value, 0
        else:
            dest = Collector.get_slave_id(var_names[0])
            return (var_names, fun_dump, initial_value), dest


    @log('Mapping')
    def map(self, var_name, fun):
        value = self.__vars[var_name]
        if isinstance(value, int):
            self.__vars[var_name] = fun(value)
        else:
            for i in range(len(value)):
                value[i] = fun(value[i])


    @log('Filtering')
    def filter(self, var_name, fun):
        value = self.__vars[var_name]
        if isinstance(value, int):
            if not fun(value):
                self.free_var(var_name)
        else:
            self.__vars[var_name] = list(filter(fun, value))


    @log('Allocating')
    def allocate_var(self, value):
        var_name = '{}-{}'.format(self.rank, self.__counter)
        self.__vars[var_name] = value
        self.__counter += 1

        return var_name


    @log('Reading')
    def read_var(self, var_id):
        return self.__vars[var_id]


    @log('Modifying')
    def modify_var(self, var_id, new_value):
        if var_id in self.__vars:
            self.__vars[var_id] = new_value
            self.log('Modify: Value changed.')
            return True

        self.log('Modify: Value not found.')
        return False


    @log('Freeing')
    def free_var(self, *var_ids):
        for var_id in var_ids:
            self.__vars.pop(var_id, None)


    @classmethod
    def get_slave_id(self, var_name):
        if not isinstance(var_name, str):
            raise ValueError("""The var_name must be a 'str'
                                not a {}.""".format(type(var_name).__name__))

        return int(var_name.split('-')[0])

    @log('Exiting')
    def quit(self, exit_code=0):
        exit(exit_code)