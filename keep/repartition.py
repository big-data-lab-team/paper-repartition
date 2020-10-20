import datetime
import math
import time
import os
from argparse import ArgumentParser
from ast import literal_eval as make_tuple
from keep import keep
from keep.partition import Partition
from keep.log import log


def main(args=None):
    parser = ArgumentParser()

    parser.add_argument(
        "A", action="store", help="shape of the reconstructed array"
    )
    parser.add_argument(
        "I",
        action="store",
        help="shape of the input blocks. Input blocks "
        "called 'in...' must be stored on disk",
    )
    parser.add_argument(
        "O",
        action="store",
        help="shape of the outut blocks. Output blocks"
        " called 'out...' will be created on disk",
    )
    commands = parser.add_mutually_exclusive_group()
    commands.add_argument(
        "--create",
        action="store_true",
        help="create input blocks on disk" " before repartitioning.",
    )
    commands.add_argument(
        "--repartition",
        action="store_true",
        help="repartition input blocks to output block dimensions",
    )
    commands.add_argument(
        "--delete",
        action="store_true",
        help="delete output blocks after repartitioning.",
    )
    commands.add_argument(
        "--test-data",
        action="store_true",
        help="reconstruct array from input blocks, "
        "reconstruct array from output blocks, "
        "check that data is identical in both "
        "reconstructions.",
    )
    parser.add_argument(
        "--max-mem", action="store", help="max memory to use, in bytes"
    )
    parser.add_argument(
        "method",
        action="store",
        help="repartitioning method to use",
        choices=["baseline", "keep"],
    )

    args, params = parser.parse_known_args(args)
    mem = args.max_mem
    if mem is not None:
        mem = int(mem)

    repart_func = {"baseline": keep.baseline, "keep": keep.keep}

    array = Partition(make_tuple(args.A), name="array")

    if args.create:
        fill = "random"
        log("Creating input blocks", 1)
    else:
        fill = None
        log("Using existing input blocks", 1)

    in_blocks = Partition(
        make_tuple(args.I), name="in", array=array, fill=fill
    )
    in_blocks.clear()

    if not args.create:
        out_blocks = Partition(make_tuple(args.O), name="out", array=array)

        # Repartitioning
        if args.repartition:
            log("Repartitioning input blocks into output blocks", 1)
            out_blocks.delete()
            out_blocks.clear()  # shouldn't be necessary but just in case
            start = time.time()
            (
                total_bytes,
                seeks,
                peak_mem,
                read_time,
                write_time,
            ) = in_blocks.repartition(
                out_blocks, mem, repart_func[args.method]
            )
            end = time.time()
            total_time = end - start
            assert total_time > read_time + write_time
            assert total_bytes == 2 * math.prod(array.shape)
            log(
                f"Seeks, peak memory (B), read time (s),"
                f" write time (s), elapsed time (s):"
                + os.linesep
                + f"{seeks},{peak_mem},{round(read_time,2)},"
                f"{round(write_time,2)},{round(total_time,2)}",
                2,
            )

        if args.test_data:
            log("Testing data", 1)
            in_blocks.repartition(array, mem, repart_func[args.method])
            with open(array.blocks[(0, 0, 0)].file_name, "rb") as f:
                in_data = f.read()
            array.delete()
            out_blocks.repartition(array, mem, repart_func[args.method])
            with open(array.blocks[(0, 0, 0)].file_name, "rb") as f:
                out_data = f.read()
            assert in_data == out_data

        if args.delete:
            log("Deleting output blocks", 1)
            out_blocks.delete()


if __name__ == "__main__":
    main()
