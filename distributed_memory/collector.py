import logging

from mpi4py import MPI
import mpi4py
import numpy as np

from .tags import Tags


class Collector:
    def __init__(self):
        self.comm = MPI.COMM_WORLD
        self.rank = self.comm.Get_rank()
        self.size = self.comm.Get_size()

        self.__counter = 0
        self.__vars = dict()
        self.log = logging.getLogger(' SLAVE-{}'.format(self.rank)).debug


    def run(self):
        while True:
            status = MPI.Status()
            msg = self.comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG,
                                 status=status)

            source = status.Get_source()
            tag = status.Get_tag()
            action = Tags.name(tag)

            self.log('Recv: Tag: {} Source: {}.'.format(tag, source))

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
            elif action == 'quit':
                self.log('exiting...')
                exit(0)
            else:
                logging.critical('Unkown tag {}:{}.'.format(tag, action))
                raise ValueError('Unkown tag {}:{}.'.format(tag, action))



    def allocate_var(self, value):
        self.log('Alloc: new id: {}'.format(self.__counter))
        self.__vars[self.__counter] = value
        self.__counter += 1

        return '{}-{}'.format(self.rank, self.__counter-1)


    def read_var(self, var_id):
        self.log('Read: Id: {}.'.format(var_id))

        val = self.__vars.get(var_id, None)
        self.log('Read: Val: {}.'.format(val))

        return val


    def modify_var(self, var_id, new_value):
        self.log('Modify: Id: {} Val: {}'.format(var_id, new_value))

        if var_id in self.__vars:
            self.__vars[var_id] = new_value
            self.log('Modify: Value changed.')
            return True

        self.log('Modify: Value not found.')
        return False


    def free_var(self, *var_ids):
        self.log('Free: {} values.'.format(len(var_ids)))

        for var_id in var_ids:
            self.__vars.pop(var_id, None)
