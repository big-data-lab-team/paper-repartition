import math
import collections
from keep.partition import Partition
from keep.block import Block
from keep.cache import KeepCache, BaselineCache
from keep.log import log


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
    # TODO: creating a new partition makes memory estimates correct,
    # but it adds an in-memory copy, this could be fixed
    return (Partition(in_blocks.shape, 'read_blocks', array),
            BaselineCache(),
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

    r, peak_mem = find_shape_with_constraint(in_blocks, out_blocks, m)
    read_blocks = Partition(r, 'read_blocks', array=array)
    write_blocks, cache = create_write_blocks(read_blocks, out_blocks)
    # Technically this count is not necessary
    seeks = keep_seek_count(in_blocks, read_blocks,
                            write_blocks, out_blocks)
    return read_blocks, cache, seeks, peak_mem


'''
    Utils
'''


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
    cache = KeepCache(out_blocks, match)

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


def find_shape_with_constraint(in_blocks, out_blocks, m):
    '''
    Search for a read block shape that respects memory constraint m
    '''

    assert(in_blocks.ndim == 3), 'Only supports dimension 3'

    # r_hat is the best shape, if it fits in memory or there is no memory
    # constraint, return it
    r_hat = get_r_hat(in_blocks, out_blocks)
    log(f'keep: rhat is {r_hat}')
    mc = peak_memory(r_hat, in_blocks, out_blocks)
    if m is None or mc <= m:
        return r_hat, mc

    array = in_blocks.array

    # evaluate nmax shapes of the form (divs0[i], r_hat[1], r_hat[2])
    divs0 = sorted([x for x in divisors(array.shape[0]) if x <= r_hat[0]],
                   reverse=True)
    nmax = len(divs0)
    ind = None

    for i in range(min(nmax, len(divs0))):
        shape = (divs0[i], r_hat[1], r_hat[2])
        log(f'Evaluating shape {shape}, memory constraint is {m}', 1)
        mc = peak_memory(shape, in_blocks, out_blocks)
        log(f'Memory estimate: {mc}B', 1)
        if(mc <= m):
            ind = i
            break
    if ind is not None:
        return (divs0[ind], r_hat[1], r_hat[2]), mc

    # We're going to have to seek in the second dimension, let's just give up
    assert(False), "Cannot find read shape that satisfies memory constraint"


def get_r_hat(in_blocks, out_blocks):
    from math import ceil as c
    inb = in_blocks
    r_hat = tuple([inb.shape[i]*(c(out_blocks.shape[i]/in_blocks.shape[i]))
                  for i in range(in_blocks.ndim)])
    array = in_blocks.array
    message = 'Cannot find r hat'
    assert(all(array.shape[i] % r_hat[i] == 0 for i in (0, 1, 2))), message
    return r_hat


def get_F_blocks(write_block, out_blocks, get_data=False, dry_run=False):
    '''
    Assuming out_blocks are of uniform size
    '''

    # F0 is where the write_block origin is
    origin = write_block.origin
    shape = write_block.shape
    out_ends = partition_to_end_coords(out_blocks)

    # TODO: turn to list comprehension
    block_ends = list(origin)
    for d in (0, 1, 2):
        ends = sorted(out_ends[d])
        for o in ends:
            if o > origin[d] and o <= origin[d] + shape[d] - 1:
                block_ends[d] = o

    shape = [block_ends[i] - origin[i] + 1 for i in range(len(origin))]
    F0 = Block(origin, shape)
    if get_data:
        F0 = write_block.get_data_block(F0, dry_run)

    # F1
    origin = (F0.origin[0], F0.origin[1], F0.origin[2] + F0.shape[2])
    shape = [F0.shape[0], F0.shape[1], write_block.shape[2] - F0.shape[2]]
    F1 = Block(origin, shape)
    if get_data:
        F1 = write_block.get_data_block(F1, dry_run)

    # F2
    origin = (F0.origin[0], F0.origin[1] + F0.shape[1], F0.origin[2])
    shape = [F0.shape[0], write_block.shape[1] - F0.shape[1], F0.shape[2]]
    F2 = Block(origin, shape)
    if get_data:
        F2 = write_block.get_data_block(F2, dry_run)

    # F3
    origin = (F0.origin[0], F0.origin[1] + F0.shape[1],
              F0.origin[2] + F0.shape[2])
    shape = [F0.shape[0], write_block.shape[1] - F0.shape[1],
             write_block.shape[2] - F0.shape[2]]
    F3 = Block(origin, shape)
    if get_data:
        F3 = write_block.get_data_block(F3, dry_run)

    # F4
    origin = (F0.origin[0] + F0.shape[0], F0.origin[1], F0.origin[2])
    shape = [write_block.shape[0] - F0.shape[0], F0.shape[1], F0.shape[2]]
    F4 = Block(origin, shape)
    if get_data:
        F4 = write_block.get_data_block(F4, dry_run)

    # F5
    origin = (F0.origin[0] + F0.shape[0], F0.origin[1],
              F0.origin[2] + F0.shape[2])
    shape = [write_block.shape[0] - F0.shape[0], F0.shape[1],
             write_block.shape[2] - F0.shape[2]]
    F5 = Block(origin, shape)
    if get_data:
        F5 = write_block.get_data_block(F5, dry_run)

    # F6
    origin = (F0.origin[0] + F0.shape[0], F0.origin[1] + F0.shape[1],
              F0.origin[2])
    shape = [write_block.shape[0] - F0.shape[0],
             write_block.shape[1] - F0.shape[1], F0.shape[2]]
    F6 = Block(origin, shape)
    if get_data:
        F6 = write_block.get_data_block(F6, dry_run)

    # F7
    origin = (F0.origin[0] + F0.shape[0], F0.origin[1] + F0.shape[1],
              F0.origin[2] + F0.shape[2])
    shape = [write_block.shape[0] - F0.shape[0],
             write_block.shape[1] - F0.shape[1],
             write_block.shape[2] - F0.shape[2]]
    F7 = Block(origin, shape)
    if get_data:
        F7 = write_block.get_data_block(F7, dry_run)

    # Remove empty blocks and return
    f_blocks = [f if not f.empty() else None
                for f in [F0, F1, F2, F3, F4, F5, F6, F7]]
    return f_blocks


def merge_blocks(block_list):
    '''
    Assume block list merges in a cuboid block.
    Assume all blocks are empty.
    Return the merged blocks.
    '''
    message = 'Cannot merge non-empty blocks'
    assert(all(b.data.mem_usage() == 0 for b in block_list)), message
    origin = tuple(min([b.origin[i] for b in block_list]) for i in (0, 1, 2))
    end = tuple(max([b.origin[i] + b.shape[i]
                for b in block_list]) for i in (0, 1, 2))
    shape = tuple(end[i] - origin[i] for i in (0, 1, 2))
    b = Block(origin, shape, data=bytearray())
    return b


def peak_memory(read_shape, in_blocks, out_blocks):
    '''
    Return the estimated amount of memory required to repartition in_blocks
    into out_blocks, using read_blocks and write_blocks.
    '''

    # To estimate the amount of memory required, we run a dry run of the
    # repartitioning

    read_blocks = Partition(read_shape, 'read_blocks', array=in_blocks.array)
    _, cache = create_write_blocks(read_blocks, out_blocks)

    def local_get_read_blocks_and_cache(in_blocks, out_blocks, m, array):
        return read_blocks, cache, None, None

    (_, _, peak_mem,
     _, _) = in_blocks.repartition(out_blocks, None,
                                   local_get_read_blocks_and_cache,
                                   dry_run=True)
    in_blocks.clear()
    out_blocks.clear()
    return peak_mem


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
    Return: end coordinates of the blocks, in each dimension.
    Example: ([500, 1000, 1500, 2000, 2500, 3000, 3500],
              [500, 1000, 1500, 2000, 2500, 3000, 3500],
              [500, 1000, 1500, 2000, 2500, 3000, 3500])
    '''

    return tuple(  # this isn't so efficient...
            sorted(set([p.blocks[b].origin[i] + p.blocks[b].shape[i] - 1
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
                   if (block.origin[d] <= m
                   and m < block.origin[d] + block.shape[d] - 1)])
              for d in (0, 1, 2))
    shape = block.shape
    if c[2] != 0:
        return (c[2] + 1)*shape[0]*shape[1]

    if c[1] != 0:
        return (c[1] + 1)*shape[0]

    if c[0] != 0:
        return c[0] + 1

    return 1
