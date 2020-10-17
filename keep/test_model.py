import keep
from partition import Partition
array = Partition((2, 2, 2), name='array', fill='random')
array.write()
out_blocks = Partition((2, 1, 2), name='out', array=array)

best_read_blocks, best_cache, min_seeks, peak_mem = keep.keep(array, out_blocks, None, array)
assert(min_seeks == 3)

array.repartition(out_blocks, None, keep.baseline)

rein_blocks = Partition((2, 2, 2), name='rein')

print('##########')

best_read_blocks, best_cache, min_seeks, peak_mem = keep.baseline(out_blocks, array, None, array)
print('########## seeks = ', min_seeks)

# out_blocks.repartition(rein_blocks, None, keep.baseline)
