#!/usr/bin/env python3

import argparse
import logging
import sys

from mpi4py import MPI

import distributed_memory as dm


def parse_args(argv):
    """Parse arguments and call the Master."""
    parser = argparse.ArgumentParser(description='Demo of the distributed memory:')
    parser.add_argument('--size', action='store', type=int, dest='size',
                        help='Size of the list stored on the distributed memory.',
                        default=20)
    parser.add_argument('--verbose', action='store_true', dest='verbose',
                        help='Verbose mode.')
    parser.add_argument('--log', action='store', type=str, dest='log',
                        choices=['critical', 'debug', 'info'],
                        default='critical', help='Log level.')

    args = parser.parse_args(argv)

    if args.log == 'critical':
        logging.basicConfig(level=logging.CRITICAL)
    elif args.log == 'debug':
        logging.basicConfig(level=logging.DEBUG)
    elif args.log == 'info':
        logging.basicConfig(level=logging.INFO)

    nb_slaves = MPI.COMM_WORLD.Get_size() - 1
    max_per_slave = args.size / nb_slaves
    mem = dm.init_memory(max_per_slave=max_per_slave)

    demo(mem, args.size, args.verbose)


def demo(mem, size, verbose):
    var = mem.add(list(range(size)))
    if verbose:
        print('Initial value:', mem.read(var))

    mem.map(var, lambda x: x ** 2)
    if verbose:
        print('After squaring:', mem.read(var))

    mem.filter(var, lambda x: x % 2 != 0)
    if verbose:
        print('After removing even integer:', mem.read(var))

    val = mem.reduce(var, lambda x, y: x * y, 1)
    if verbose:
        print('Multiplication of the remaining elements:', val)

    mem.quit()

if __name__ == '__main__':
    assert MPI.COMM_WORLD.Get_size() > 1, 'Provide at least two processus.'

    parse_args(sys.argv[1:])
