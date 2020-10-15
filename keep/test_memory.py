import keep
from partition import Partition
from block import Block
import math


def repartition(in_blocks, out_blocks):
    m = math.prod(in_blocks.shape)
    print(f'Theoretical mem consumption: {m/1000000}MiB')
    in_blocks.repartition(out_blocks, -1, keep.baseline)


@profile
def test_repartition_keep_2():
    (a, b, c) = (500, 400, 600)
    # Prepare in-blocks
    array = Partition((a, b, c), name='array')
    in_blocks = Partition((a, b, c), name='in', array=array) #, fill='random')
    # in_blocks.write()
    # in_blocks.clear()

    # Repartittion in-blocks
    out_blocks = Partition((a, int(b/2), c), name='out', array=array)
    repartition(in_blocks, out_blocks)


if __name__ == '__main__':
    test_repartition_keep_2()
