from keep import peak_memory, create_write_blocks
from partition import Partition

def test_peak_mem():
    array = Partition((6, 6, 6), name='array')
    in_blocks = Partition((3, 3, 3), name='in', array=array)
    out_blocks = Partition((2, 2, 2), name='in', array=array)

    r = (3, 3, 3)
    read_blocks = Partition(r, 'read_blocks', array=array)
    write_blocks, _ = create_write_blocks(read_blocks, out_blocks)

    pm = peak_memory(array, in_blocks, write_blocks)
    print('Peak mem:', pm)

test_peak_mem()