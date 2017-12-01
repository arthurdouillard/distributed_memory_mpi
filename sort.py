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
    parser = argparse.ArgumentParser(description='test')
    parser.add_argument('--low', action='store', type=int, dest='low',
                        default=-1000,
                        help='Lowest possible value of the random array.')
    parser.add_argument('--high', action='store', type=int, dest='high',
                        default=1000,
                        help='Highest possible value of the random array.')
    parser.add_argument('--size', action='store', type=int, dest='size',
                        default=10,
                        help='Size of the random array')
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
    sort(mem, args)


def sort(mem, args):
    var1= mem.add(1)
    var2 = mem.add(2)

    print(var1, var2)

    print(mem.read(var1), mem.read(var2))

    mem.quit()


if __name__ == '__main__':
    assert MPI.COMM_WORLD.Get_size() > 1, 'Provide at least two processus.'

    parse_args(sys.argv[1:])
