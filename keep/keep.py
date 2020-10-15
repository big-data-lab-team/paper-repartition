import math
import collections
from partition import Partition
from block import Block
from cache import KeepCache, BaselineCache

'''
This module is the entrypoint for array repartitioning. 
'''


def baseline(in_blocks, out_blocks, m, array):
    '''
    Implements get_read_blocks_and_cache(in_blocks, out_blocks, m, array)
    used in Partition.repartition. It provides a baseline repartitioning
    algorithm where read blocks are input blocks.

    Arguments:
        in_blocks: input partition, to be repartitioned
        out_blocks: output partition, to be written to disk
        m: max memory to be used by the repartitioning. This parameter is
           here for type consistency in Partition.repartition but it is
           ignored in this baseline implementation.
        array: partitioned array. This parameter is here for type
               consistency in Partition.repartition but it is ignored in this
               baseline implementation.
    '''
    return (in_blocks, BaselineCache(),
            baseline_seek_count(in_blocks, out_blocks),
            math.prod(in_blocks.shape))


def keep(in_blocks, out_blocks, m, array):
    '''
    Implements get_read_blocks_and_cache(in_blocks, out_blocks, m, array)
    used in Partition.repartition. Implements the keep heuristic (Algorithm
    2 in the paper).

    Arguments:
        in_blocks: input partition, to be repartitioned
        out_blocks: output partition, to be written to disk
        m: max memory to be used by the repartitioning. If None, memory
           constraint is ignored.
        array: partitioned array. Doesn't need to contain data, used just
               to get total dimensions of the array.
    '''

    r_hat = get_r_hat(in_blocks, out_blocks)
    read_shapes = candidate_read_shapes(in_blocks, out_blocks, r_hat, array)
    min_seeks = None
    for r in read_shapes:
        read_blocks = Partition(r, 'read_blocks', array=array)
        write_blocks, cache = create_write_blocks(read_blocks, out_blocks)
        mc = peak_memory(array, read_blocks, write_blocks)
        if m is not None and mc > m:
            continue
        seeks = keep_seek_count(in_blocks, read_blocks,
                                write_blocks, out_blocks)
        if r == r_hat:  # fix that block
            return read_blocks, cache, seeks, mc
        if min_seeks is None or seeks < min_seeks:
            best_read_blocks = read_blocks
            best_cache = cache
            min_seeks = seeks
            peak_mem = mc
    assert(min_seeks is not None), ('Cannot find read shape that fullfills'
                                    ' memory constraint')
    return best_read_blocks, best_cache, min_seeks, peak_mem


'''
    Utils
'''


def candidate_read_shapes(in_blocks, out_blocks, r_hat, array):
    assert(in_blocks.ndim == 3), 'Only supports dimension 3'
    log(f'keep: rhat is {r_hat}')
    divs0 = [x for x in divisors(array.shape[0]) if x <= r_hat[0]]
    divs1 = [x for x in divisors(array.shape[1]) if x <= r_hat[1]]
    shapes = [ (x, y, r_hat[2]) for x in divs0 for y in divs1 ]
    shapes.sort(key=lambda x: (x[1], x[0]),reverse=True)
    log(f'keep: shapes = {shapes}')
    return shapes


def create_write_blocks(read_blocks, out_blocks):
    '''
        read_block: partition
        out_blocks: partition
    '''

    match = {}

    moved_f_blocks = [[] for i in range(len(read_blocks.blocks))]

    for i, r in enumerate(read_blocks.blocks):
        f_blocks = get_F_blocks(read_blocks.blocks[r], out_blocks,
                                get_data=False)

        moved_f_blocks[i] += [f_blocks[0]]  # don't move F0
        match[(r, 0)] = i
        for f in range(1, 8):
            if not f_blocks[f] is None:
                destF0 = destination_F0(read_blocks, i, f)
                moved_f_blocks[destF0] += [f_blocks[f]]
                match[(r, f)] = destF0

    merged_blocks = [merge_blocks(blocks) for blocks in moved_f_blocks]
    match = {k: merged_blocks[match[k]] for k in match}
    blocks = {m.origin: m for m in merged_blocks}

    # Warning: write_blocks are a partition but a non-uniform one
    # This may have side effects. This is also the reason for the
    # weird create_blocks param
    write_blocks = Partition((1, 1, 1),
                             name='write_blocks',
                             array=read_blocks.array, create_blocks=False)
    write_blocks.blocks = blocks
    cache = KeepCache(write_blocks, out_blocks, match)

    return write_blocks, cache


def destination_F0(read_blocks, read_block_ind, F_ind):
    '''
        read_blocks: partition
        read_block_ind: read block index
        F_ind: the i in Fi
    '''
    neighbor_x0 = read_blocks.get_neighbor_block_ind(read_block_ind, 0)
    neighbor_x1 = read_blocks.get_neighbor_block_ind(read_block_ind, 1)
    neighbor_x2 = read_blocks.get_neighbor_block_ind(read_block_ind, 2)
    assert(F_ind < 8 and F_ind > 0)
    if F_ind == 1:
        return neighbor_x2
    if F_ind == 2:
        return neighbor_x1
    if F_ind == 3:
        return destination_F0(read_blocks, neighbor_x1, 1)
    if F_ind == 4:
        return neighbor_x0
    if F_ind == 5:
        return destination_F0(read_blocks, neighbor_x0, 1) 
    if F_ind == 6:
        return destination_F0(read_blocks, neighbor_x0, 2)
    if F_ind == 7:
        return destination_F0(read_blocks, neighbor_x0, 3)


def divisors(n):
    return [x for x in range(1, n+1) if n % x == 0]


def get_r_hat(in_blocks, out_blocks):
    from math import ceil as c
    return tuple([in_blocks.shape[i]*(c(out_blocks.shape[i]/in_blocks.shape[i]))
                  for i in range(in_blocks.ndim)])


def get_F_blocks(write_block, out_blocks, get_data=False):
    '''
    Assuming out_blocks are of uniform size
    '''

    # F0 is where the write_block origin is
    origin = write_block.origin
    out_ends = [ math.floor((write_block.origin[i]+write_block.shape[i])/out_blocks.shape[i])*out_blocks.shape[i] for i in range(len(origin)) ]
    shape = [ out_ends[i]-write_block.origin[i] if out_ends[i] > write_block.origin[i] else write_block.shape[i] for i in range(len(origin))] 
    F0 = Block(origin, shape)
    if get_data:
        F0 = write_block.get_data_block(F0)

    # F1
    origin = (F0.origin[0], F0.origin[1], F0.origin[2] + F0.shape[2])
    shape = [ F0.shape[0], F0.shape[1], write_block.shape[2] - F0.shape[2] ]
    F1 = Block(origin, shape)
    if get_data:
        F1 = write_block.get_data_block(F1)

    # F2
    origin = (F0.origin[0], F0.origin[1] + F0.shape[1], F0.origin[2])
    shape = [ F0.shape[0], write_block.shape[1] - F0.shape[1], F0.shape[2] ]
    F2 = Block(origin, shape)
    if get_data:
        F2 = write_block.get_data_block(F2)

    # F3
    origin = (F0.origin[0], F0.origin[1] + F0.shape[1], F0.origin[2] + F0.shape[2])
    shape = [ F0.shape[0], write_block.shape[1] - F0.shape[1], write_block.shape[2] - F0.shape[2] ]
    F3 = Block(origin, shape)
    if get_data:
        F3 = write_block.get_data_block(F3)

    # F4
    origin = ( F0.origin[0] + F0.shape[0], F0.origin[1], F0.origin[2] )
    shape = [ write_block.shape[0] - F0.shape[0], F0.shape[1], F0.shape[2] ]
    F4 = Block(origin, shape)
    if get_data:
        F4 = write_block.get_data_block(F4)

    # F5
    origin = ( F0.origin[0] + F0.shape[0], F0.origin[1], F0.origin[2] + F0.shape[2] )
    shape = [ write_block.shape[0] - F0.shape[0], F0.shape[1], write_block.shape[2] - F0.shape[2] ]
    F5 = Block(origin, shape)
    if get_data:
        F5 = write_block.get_data_block(F5)

    # F6
    origin = ( F0.origin[0] + F0.shape[0], F0.origin[1] + F0.shape[1], F0.origin[2] )
    shape = [ write_block.shape[0] - F0.shape[0], write_block.shape[1] - F0.shape[1], F0.shape[2] ]
    F6 = Block(origin, shape)
    if get_data:
        F6 = write_block.get_data_block(F6)

    # F7
    origin = ( F0.origin[0] + F0.shape[0], F0.origin[1] + F0.shape[1], F0.origin[2] + F0.shape[2] )
    shape = [ write_block.shape[0] - F0.shape[0], write_block.shape[1] - F0.shape[1], write_block.shape[2] - F0.shape[2] ]
    F7 = Block(origin, shape)
    if get_data:
        F7 = write_block.get_data_block(F7)

    # Remove empty blocks and return
    f_blocks = [ f if not f.empty() else None for f in [F0, F1, F2, F3, F4, F5, F6, F7]]
    return f_blocks


def merge_blocks(block_list):
    '''
    Assume block list merges in a cuboid block.
    Assume all blocks are empty.
    Return the merged blocks.
    '''
    assert(all(b.data.mem_usage() == 0 for b in block_list)), 'Cannot merge non-empty blocks'
    origin = tuple(min([b.origin[i] for b in block_list]) for i in (0, 1, 2))
    end = tuple(max([b.origin[i] + b.shape[i] for b in block_list]) for i in (0, 1, 2))
    shape = tuple(end[i] - origin[i] for i in (0, 1, 2))
    b = Block(origin, shape, data=bytearray())
    return b


def peak_memory(array, read_blocks, write_blocks):
    peak = 0
    b1 = collections.deque(maxlen=1)

    size2 = int(array.shape[2]/read_blocks.shape[2])
    b2 = collections.deque(maxlen=size2)

    size3 = int(array.shape[2]/read_blocks.shape[2] *
                array.shape[1]/read_blocks.shape[1])
    b3 = collections.deque(maxlen=size3)

    def append_f_blocks(buffer, f_indices, f_blocks):
        size = sum([math.prod(f_blocks[i].shape)
                    if f_blocks[i] is not None else 0
                    for i in f_indices
                    ])
        buffer.append(size)

    for b in read_blocks.blocks:
        f_blocks = get_F_blocks(read_blocks.blocks[b], write_blocks)
        append_f_blocks(b1, (1,), f_blocks)
        append_f_blocks(b2, (2, 3), f_blocks)
        append_f_blocks(b3, (4, 5, 6, 7), f_blocks)
        peak = max(peak, sum(b1) + sum(b2) + sum(b3))

    return peak


'''
    Seek model
'''


def baseline_seek_count(in_blocks, out_blocks):
    return keep_seek_count(in_blocks, in_blocks, in_blocks, out_blocks)


def keep_seek_count(in_blocks, read_blocks, write_blocks, out_blocks):
    return (seek_count(read_blocks, in_blocks) +
            seek_count(write_blocks, out_blocks))


def partition_to_end_coords(p):
    '''
    p: a partition
    Return: end coordinates of the blocks, in each dimension. Example: ([500, 1000, 1500, 2000, 2500, 3000, 3500],
                                                                   [500, 1000, 1500, 2000, 2500, 3000, 3500],
                                                                   [500, 1000, 1500, 2000, 2500, 3000, 3500])
    '''

    return tuple(  # this isn't so efficient...
            sorted(set([p.blocks[b].origin[i] + p.blocks[b].shape[i]
                        for b in p.blocks]))
            for i in (0, 1, 2)
    )


def seek_count(memory_blocks, disk_blocks):
    '''
    memory_blocks: a partition representing blocks stored in memory, to be
                   written to disk_blocks or to be read from disk_blocks.
    disk_blocks: a partition representing blocks to be written to disk from
                 memory_blocks, or to be read from disk into memory_blocks
    Returns: number of seeks required to write memory_blocks into disk_blocks.
             This number is also the number of seeks
             to read disk_blocks into memory_blocks.
    '''

    M = partition_to_end_coords(memory_blocks)
    s = sum([seek_count_block(disk_blocks.blocks[b], M)
            for b in disk_blocks.blocks])
    return s


def seek_count_block(block, M):
    '''
    Return the number of seeks required to write block from M, or to read M
    from block.
    '''

    # Cuts
    c = tuple(len([m for m in M[d]
                   if (block.origin[d] < m
                   and m < block.origin[d] + block.shape[d])])
              for d in (0, 1, 2))

    shape = block.shape
    if c[2] != 0:
        return (c[2] + 1)*shape[0]*shape[1]

    if c[1] != 0:
        return (c[1] + 1)*shape[0]

    if c[0] != 0:
        return c[0] + 1

    return 1


def log(message, level=0):
    LOG_LEVEL = 0
    if level >= LOG_LEVEL:
        print(message)
