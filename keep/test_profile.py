import keep
from partition import Partition


def log(message):
    import datetime
    print(f'[ {datetime.datetime.now().time()} ] {message}')


log('Creating array')
array = Partition((500, 500, 500), name='array', fill='random')
log('Writing array')
array.write()
log('Creating input blocks')
in_blocks = Partition((100, 100, 100), name='in', array=array)
log('Repartitioning array into input blocks')
array.repartition(in_blocks, None, keep.keep)

log('Clearing input blocks in mem')
in_blocks.clear()
log('Creating output blocks')
out_blocks = Partition((50, 50, 50), name='out', array=array)
log('Repartitioning input blocks into output blocks')
total_bytes, seeks, peak_mem = in_blocks.repartition(out_blocks,
                                            None, keep.keep)
log(f'Estimated peak memory: {peak_mem}B')
log(f'Number of seeks: {seeks}')

log('Repartitioning output blocks into initial array')
rein = Partition((500, 500, 500), name='rein') 
total_bytes, seeks, peak_mem = out_blocks.repartition(rein, None, keep.keep)
log(f'Estimated peak memory: {peak_mem}B')
log(f'Number of seeks: {seeks}')