#!/usr/bin/env python3

import argparse
import logging
import sys

from mpi4py import MPI

import distributed_memory as dm


def parse_args(argv):
    """Parse arguments and call the Master.

    For help, call:
        $ python -m distributed_memory -h
    """
    parser = argparse.ArgumentParser(description='Demo of the distributed memory')
    parser.add_argument('--mem', action='store', type=int, dest='mem',
                        default=10, help='Max size in elements for each processus.')
    parser.add_argument('--log', action='store', type=str, dest='log',
                        choices=['critical', 'debug', 'info'],
                        default='critical', help='Log level')

    args = parser.parse_args(argv)

    if args.log == 'critical':
        logging.basicConfig(level=logging.CRITICAL)
    elif args.log == 'debug':
        logging.basicConfig(level=logging.DEBUG)
    elif args.log == 'info':
        logging.basicConfig(level=logging.INFO)

    mem = dm.init_memory(max_per_slave=args.mem)
    demo(mem)


def demo(mem):
    var = mem.add(list(range(10)))
    print('Initial value:', mem.read(var))

    mem.map(var, lambda x: x ** 2)
    print('After squaring:', mem.read(var))

    mem.filter(var, lambda x: x % 2 != 0)
    print('After removing even integer:', mem.read(var))

    val = mem.reduce(var, lambda x, y: x * y, 1)
    print('Multiplication of the remaining elements:', val)

    mem.quit()

if __name__ == '__main__':
    assert MPI.COMM_WORLD.Get_size() > 1, 'Provide at least two processus.'

    parse_args(sys.argv[1:])
