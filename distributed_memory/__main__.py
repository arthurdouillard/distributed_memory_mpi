import argparse
import logging
import sys

from .master import Master

logging.basicConfig(level=logging.DEBUG)

def parse_args(argv):
    """Parse arguments and call the Master.

    For help, call:
        $ python -m distributed_memory -h
    """
    parser = argparse.ArgumentParser(description='test')
    parser.add_argument('-l', '--low', action='store', type=int, dest='low',
                        help='Lowest possible value of the random array.')
    parser.add_argument('-h', '--high', action='store', type=int, dest='high',
                        help='Highest possible value of the random array.')
    parser.add_argument('-s', '--size', action='store', type=int, dest='size',
                        help='Size of the random array')
    parser.add_argument('-l', '--log', action='store', type=str, dest='log',
                        choices=['critical', 'debug', 'info'],
                        default='critical', help='Log level')

    args = parser.parse_args(argv)

    if args.log == 'critical':
        logging.basicConfig(level=logging.CRITICAL)
    elif args.log == 'debug':
        logging.basicConfig(level=logging.DEBUG)
    elif args.log == 'info':
        logging.basicConfig(level=logging.INFO)

    master = Master(range_vals=(args.low, args.high), size=args.size)


if __name__ == '__main__':
    parse_args(sys.argv[1:])
