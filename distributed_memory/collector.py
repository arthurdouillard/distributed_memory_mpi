import logging

from mpi4py import MPI
import numpy as np

from .tags import Tags


class Collector:
    def __init__(self):
        self.comm = MPI.COMM_WORLD
        self.rank = self.comm.Get_rank()
        self.size = self.comm.Get_size()

        self.__counter = 0
        self.__vars = dict()
        self.logger = logging.getLogger(' ID-{}'.format(self.rank))


    def logic(self):
        while True:
            status = MPI.Status()
            msg = self.comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG,
                                 status=status)

            source = status.Get_source()
            tag = status.Get_tag()
            action = Tags.name(tag)

            self.logger.info('Recv: Tag: {} Source: {}.'.format(tag, source))

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
            elif action == 'sort':
                if new_id is None:
                    logging.critical('No values allocated when asked to sort.')
                self.comm.send(self.sort(new_id), dest=source, tag=tag)
            elif action == 'quit':
                self.logger.debug('exiting...')
                exit(0)
            else:
                raise Exception('This case should not happen.')


    def sort(self, var_id, numpy=False):
        array = self.__vars.get(var_id, None)
        if array is None:
            self.logger.critical('Sort: {} id is not known.'.format(var_id))

        if numpy:
            array.sort()
            return array
        else:
            return Collector.quicksort(array)

    @classmethod
    def quicksort(cls, array):
        if array.size == 0:
            return np.array([])

        pivot = array[0]
        left = Collector.quicksort(array[np.where(array < pivot)[0]])
        right = Collector.quicksort(array[np.where(array >= pivot)[0][1:]])


        return np.hstack((left, pivot, right))



    def allocate_var(self, array):
        self.logger.debug('Alloc: new id: {}'.format(self.__counter))
        self.__vars[self.__counter] = array
        self.__counter += 1

        return self.__counter-1


    def read_var(self, var_id):
        self.logger.info('Read: Id: {}.'.format(var_id))

        val = self.__vars.get(var_id, None)
        self.logger.debug('Read: Val: {}.'.format(val))


    def modify_var(self, var_id, new_value):
        self.logger.debug('Modify: Id: {} Val: {}'.format(var_id, new_value))

        if var_id in self.__vars:
            self.__vars[var_id] = new_value
            self.logger.debug('Modify: Value changed.')
            return True

        self.logger.debug('Modify: Value not found.')
        return False


    def free_var(self, *var_ids):
        self.logger.debug('Free: {} values.'.format(len(var_ids)))

        for var_id in var_ids:
            self.__vars.pop(var_id, None)
