from keep import peak_memory
from partition import Partition

def test_peak_mem():
    array = Partition((12, 12, 12), name='array')
    in_blocks = Partition((4, 4, 4), name='in', array=array)
    write_blocks = Partition((3, 3, 3), name='in', array=array)
    pm = peak_memory(array, in_blocks, write_blocks)
    print(pm)

test_peak_mem()