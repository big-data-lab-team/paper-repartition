import datetime
import keep
import time
import os
from argparse import ArgumentParser
from ast import literal_eval as make_tuple
from partition import Partition


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
    parser.add_argument("method", action="store",
                        help="repartitioning method to use",
                        choices=["baseline", "keep"])

    args, params = parser.parse_known_args(args)

    repart_func = {
        'baseline': keep.baseline,
        'keep': keep.keep
    }

    array = Partition(make_tuple(args.A), name='array', fill='random')
    in_blocks = Partition(make_tuple(args.I), name='in', array=array)
    if args.create:
        log('Writing complete array')
        array.write()
        log('Creating input blocks')
        array.repartition(in_blocks, None, repart_func[args.method])
        in_blocks.clear()

    out_blocks = Partition(make_tuple(args.O), name='out', array=array)
    out_blocks.delete()
    out_blocks.clear()  # shouldn't be necessary but just in case
    log('Repartitioning input blocks into output blocks')
    start = time.time()
    (total_bytes, seeks, peak_mem,
     read_time, write_time) = in_blocks.repartition(out_blocks,
                                                    None,
                                                    repart_func[args.method])
    end = time.time()
    total_time = end - start
    assert(total_time > read_time + write_time)
    log(f'Data read/written (B), seeks, peak memory (B), read time (s),'
        f' write time (s), elapsed time (s):' + os.linesep +
        f'{total_bytes},{seeks},{peak_mem},{round(read_time,2)},'
        f'{round(write_time,2)},{round(total_time,2)}')


if __name__ == '__main__':
    main()
