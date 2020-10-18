import keep
import datetime
from partition import Partition
from argparse import ArgumentParser
from ast import literal_eval as make_tuple


def log(message):
    print(f'[ {datetime.datetime.now().time()} ] {message}')


def main(args=None):
    parser = ArgumentParser()

    parser.add_argument("A", action="store", help="shape of the reconstructed array")
    parser.add_argument("I", action="store", help="shape of the input blocks. Input blocks called 'in...' must be stored on disk")
    parser.add_argument("O", action="store", help="shape of the outut blocks. Output blocks called 'out...' will be created on disk")
    parser.add_argument("--create", action="store_true", help="create input blocks on disk before repartitioning.")
    parser.add_argument("--delete", action="store_true", help="delete output blocks after repartitioning.")
    parser.add_argument("--test-data", action="store_true", help="reconstruct array from input blocks, reconstruct array from output blocks, check that data is identical in both reconstructions.")
    # add keep vs baseline

    args, params = parser.parse_known_args(args)

    array = Partition(make_tuple(args.A), name='array', fill='random')
    in_blocks = Partition(make_tuple(args.I), name='in', array=array)
    if args.create:
        log('Writing complete array')
        array.write()
        log('Creating input blocks')
        array.repartition(in_blocks, None, keep.keep)
    out_blocks = Partition(make_tuple(args.O), name='out', array=array)
    log('Repartitioning input blocks into output blocks')
    in_blocks.repartition(out_blocks, None, keep.keep)
    log('Done')


if __name__ == '__main__':
    main()
