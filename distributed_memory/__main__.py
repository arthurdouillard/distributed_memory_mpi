import argparse
import logging
import sys

from mpi4py import MPI

from .master import Master
from .collector import Collector


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

    if MPI.COMM_WORLD.Get_rank() == 0:
        master = Master(range_vals=(args.low, args.high), size=args.size)
        master.sort()
    else:
        collector = Collector()
        collector.logic()


if __name__ == '__main__':
    assert MPI.COMM_WORLD.Get_size() > 1, 'Provide at least two processus.'

    parse_args(sys.argv[1:])
