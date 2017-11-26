import logging

from mpi4py import MPI
import numpy as np

from .tags import Tags

class Master:
    def __init__(self, range_vals=(-1000, 1000), size=1000):
        assert len(range_vals) == 2, 'range_vals must be of length 2.'

        logging.info('Starting Master with {} elements ranging {} --> {}.'\
                     .format(size, range_vals[0], range_vals[1]))

        self.comm = MPI.COMM_WORLD
        self.nb_slaves = self.comm.Get_size() - 1 # Minus 1 to avoid counting Master
        self.slaves_tracking = {i: [] for i in range(self.nb_slaves)}

        self.range_vals = range_vals
        self.size = size
        self.range = (range_vals[1] - range_vals[0]) // self.nb_slaves



    def sort(self):
        array = np.random.randint(low=self.range_vals[0],
                                  high=self.range_vals[1], size=self.size)
        print(array)
        # Send integer values to each processus by buckets
        for slave_id in range(self.nb_slaves):
            self.comm.send(array[self.choose_bucket(array, slave_id)],
                           dest=slave_id+1, tag=Tags.alloc)

        # Gather new allocated variables' ids
        for slave_id in range(self.nb_slaves):
            ids = self.comm.recv(source=slave_id+1, tag=Tags.alloc)
            self.slaves_tracking[slave_id].append(ids)

            self.comm.send(0, dest=slave_id+1, tag=Tags.sort)

        tmp = np.empty_like(array)
        for slave_id in range(self.nb_slaves):
            subarray = self.comm.recv(source=slave_id+1, tag=Tags.sort)
            print('slave: {}\n\t {}'.format(slave_id+1, subarray))

        for slave_id in range(self.nb_slaves):
            self.comm.send(0, dest=slave_id+1, tag=Tags.quit)

    def choose_bucket(self, array, slave_id):
        lower_bound = self.range_vals[0] + self.range * slave_id
        upper_bound = self.range_vals[0] + self.range * (slave_id + 1)
        return np.where((array >= lower_bound) & (array < upper_bound))[0]