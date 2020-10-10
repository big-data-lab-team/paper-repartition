import math
from partition import Partition
from block import Block
from cache import KeepCache, BaselineCache

'''
    Keep and baseline algorithms
'''

def keep(in_blocks, out_blocks, m, array):
    r_hat = get_r_hat(in_blocks, out_blocks)
    read_shapes = candidate_read_shapes(in_blocks, out_blocks, r_hat, array)
    min_seeks = None
    for r in read_shapes:
        read_blocks = Partition(r, 'read_blocks', array=array)
        write_blocks, cache = create_write_blocks(read_blocks, out_blocks)
        mc = peak_memory(r, write_blocks)
        if mc > m:
            continue
        if r == r_hat:
            best_read_blocks = read_blocks
            best_cache = cache
            break
        s = generated_seeks(in_blocks, read_blocks, write_blocks, out_blocks)
        if min_seeks == None or s < min_seeks:
            min_seeks, best_read_blocks, best_cache = s, read_blocks, cache
    return best_read_blocks, best_cache

def baseline(in_blocks, out_blocks, m, array):
    return in_blocks, BaselineCache()
 
'''
    Utils
'''

def peak_memory(r, write_blocks):
   # print(f'TODO: implement peak memory')
    return -1

def generated_seeks(in_blocks, read_blocks, write_blocks, out_blocks):
   # print(f'TODO: implement seek model')
    return -1

def get_r_hat(in_blocks, out_blocks):
    return tuple([ in_blocks.shape[i]*(
                                       math.ceil(out_blocks.shape[i]/in_blocks.shape[i]))
                                       for i in range(in_blocks.ndim)])


def candidate_read_shapes(in_blocks, out_blocks, r_hat, array):
    assert(in_blocks.ndim == 3), 'Only supports dimension 3'
    log(f'keep: rhat is {r_hat}')
    divs0 = [x for x in divisors(array.shape[0]) if x <= r_hat[0]]
    divs1 = [x for x in divisors(array.shape[1]) if x <= r_hat[1]]
    shapes = [ (x, y, r_hat[2]) for x in divs0 for y in divs1 ]
    shapes.sort(key=lambda x: (x[1], x[0]),reverse=True)
    log(f'keep: shapes = {shapes}')
    return shapes

def divisors(n):
    return [x for x in range(1, n+1) if n % x == 0]

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

def create_write_blocks(read_blocks, out_blocks):
    '''
        read_block: partition
        out_blocks: partition
    '''

    match = {}

    moved_f_blocks = [ [] for i in range(len(read_blocks.blocks))]

    for i, r in enumerate(read_blocks.blocks):
        f_blocks = get_F_blocks(read_blocks.blocks[r], out_blocks, get_data=False)

        moved_f_blocks[i] += [ f_blocks[0] ]  # don't move F0
        match[(r, 0)] = i
        for f in range(1, 8):
            if not f_blocks[f] is None:
                destF0 = destination_F0(read_blocks, i, f)
                if destF0 != i:
                    log(f'Block {i}: moving F{f} {f_blocks[f]} to Block {destF0} F0', 0)
                else:
                    log(f'Block {i}: keeping F{f} here', 0)
                moved_f_blocks[destF0] += [ f_blocks[f] ]
                match[(r, f)] = destF0

    merged_blocks = [ merge_blocks(blocks) for blocks in moved_f_blocks ]
    match = { k: merged_blocks[match[k]] for k in match}
    blocks = { m.origin: m for m in merged_blocks }


    # Warning: write_blocks are a partition but a non-uniform one
    # This is indicated by the null shape for now, this may have side effects
    write_blocks = Partition((1, 1, 1),
                             name='write_blocks',
                            array=read_blocks.array)
    write_blocks.blocks = blocks
    cache = KeepCache(write_blocks, out_blocks, match)

    return write_blocks, cache


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

'''
    Seek model
'''

def shape_to_end_coords(M, A, d=3):
    '''
    M: block shape M=(M1, M2, M3). Example: (500, 500, 500)
    A: input array shape A=(A1, A2, A3). Example: (3500, 3500, 3500)
    Return: end coordinates of the blocks, in each dimension. Example: ([500, 1000, 1500, 2000, 2500, 3000, 3500],
                                                                   [500, 1000, 1500, 2000, 2500, 3000, 3500],
                                                                   [500, 1000, 1500, 2000, 2500, 3000, 3500])
    '''
    return [ [ (j+1)*M[i] for j in range(int(A[i]/M[i])) ] for i in range(d)]

def seeks(A, M, D):
    '''
    A: shape of the large array. Example: (3500, 3500, 3500)
    M: coordinates of memory block ends (read or write). Example: ([500, 1000, 1500, 2000, 2500, 3000, 3500],
                                                                   [500, 1000, 1500, 2000, 2500, 3000, 3500],
                                                                   [500, 1000, 1500, 2000, 2500, 3000, 3500])
    D: coordinates of disk block ends (input or output). Example: ([500, 1000, 1500, 2000, 2500, 3000, 3500],
                                                                   [500, 1000, 1500, 2000, 2500, 3000, 3500],
                                                                   [500, 1000, 1500, 2000, 2500, 3000, 3500])
    Returns: number of seeks required to write M blocks into D blocks. This number is also the number of seeks
             to read D blocks into M blocks.
    '''

    c = [ 0 for i in range(len(A))] # number of cuts in each dimension
    m = [] # number of matches in each dimension

    n = math.prod( [len(D[i]) for i in range(len(A))])  # Total number of disk blocks

    for d in range(len(A)): # d is the dimension index
        
        nd = len(D[d])
        Cd = [ ]  # all the cut coordinates (for debugging and visualization)
        for i in range(nd): # for each output block, check how many pieces need to be written
            if i == 0:
                Cid = [ m for m in M[d] if 0 < m and m < D[d][i] ]  # number of write block endings in the output block
            else:               
                Cid = [ m for m in M[d] if D[d][i-1] < m and m < D[d][i] ]  # number of write block endings in the output block
            if len(Cid) == 0:
                continue
            c[d] += len(Cid) + 1
            Cd += Cid

        m.append(len(set(M[d]).union(set(D[d]))) - c[d])

    s = A[0]*A[1]*c[2] + A[0]*c[1]*m[2] + c[0]*m[1]*m[2] + n# + math.prod([m[i] + c[i] for i in (0, 1, 2)])

    return s

def log(message, level=0):
    LOG_LEVEL=0
    if level >= LOG_LEVEL:
        print(message)