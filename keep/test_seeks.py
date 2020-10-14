import keep
from partition import Partition

def test_seeks():
    array = Partition((12, 12, 12), name='array')
    in_blocks = Partition((4, 4, 4), name='in', array=array, fill='random')
    in_blocks.write()
    out_blocks = Partition((3, 3, 3), name='out', array=array)

    total_bytes, seeks = in_blocks.repartition(out_blocks, -1, keep.baseline)
    print(seeks)

    s = keep.seek_model(array, in_blocks, out_blocks)
    print(s)

test_seeks()