import keep
from partition import Partition

array = Partition((12, 12, 12), name='array', fill='random')
array.write()
array.clear()
in_blocks = Partition((4, 4, 4), name='in', array=array)
array.repartition(in_blocks, None, keep.keep)
print("######################################")

print("######################################")
in_blocks.clear()
out_blocks = Partition((3, 3, 3), name='out', array=array)
in_blocks.repartition(out_blocks, None, keep.keep)

