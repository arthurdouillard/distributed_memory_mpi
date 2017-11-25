import logging

from mpi4py import MPI

from .tags import Tags


class Collector:
    def __init__(self, verbose=False):
        self.comm = MPI.COMM_WORLD
        self.rank = self.comm.Get_rank()
        self.size = self.comm.Get_size()

        self.__counter = 0
        self.__vars = dict()
        self.verbose = verbose
        self.logger = logging.getLogger('ID-{}'.format(self.rank))


    def logic(self):
        while True:
            status = MPI.Status()
            msg = self.comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG,
                                 status=status)

            source = status.Get_source()
            tag = status.Get_tag()
            action = Tags(tag).name

            self.logger.info('Recv: Tag: {} Source: {}.'.format(tag, source))

            if action == 'alloc':
                new_ids = self.allocate_var(msg)
                self.comm.send(new_ids, dest=source, tag=tag))
            elif action == 'read':
                self.comm.send(self.read_var(msg), dest=source, tag=tag)
            elif action == 'modify':
                self.comm.send(self.modify_var(msg[0], msg[1]), dest=source,
                               tag=tag)
            elif action == 'free':
                self.free_var(msg)
            elif action == 'sort':
                pass
            elif action == 'quit':
                exit(0)
            else:
                raise Exception('This case should not happen.')


    def allocate_var(self, *values):
        self.logger.info('Alloc: {} values.'.format(len(values)))
        self.logger.debug('Alloc: {}.'.format(values))

        start_range = self.__counter

        for value in values:
            self.__vars[self.__counter] = value
            self.__counter += 1

        return list(range(start_range, self.__counter))


    def read_var(self, var_id):
        self.logger.info('Read: Id: {}.'.format(var_id))

        val = self.__vars.get(var_id, None)
        self.logger.debug('Read: Val: {}.'.format(val))


    def modify_var(self, var_id, new_value):
        self.logger.info('Modify: Id: {} Val: {}'.format(var_id, new_value))

        if var_id in self.__vars:
            self.__vars[var_id] = new_value
            self.logger.debug('Modify: Value changed.')
            return True

        self.logger.debug('Modify: Value not found.')
        return False


    def free_var(self, *var_ids):
        self.logger.info('Free: {} values.'.format(len(var_ids)))

        for var_id in var_ids:
            self.__vars.pop(var_id, None)
