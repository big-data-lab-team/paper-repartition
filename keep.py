import math
import numpy as np


class Partition():
    '''
    A uniform partition of the array to be repartitioned. May be the array
    itself (partition of size 1), the input, output, read or write blocks.

    Attributes:
        shape: the shape of the blocks in the partition
        array: a partition with 1 block, representing the partitioned array (might be None)
        blocs: the list of blocks in the partition
        zeros: if True, fill all the blocks with zeros. Warning: this allocates memory. 
    '''
    def __init__(self, shape, name, array=None, zeros=False, element_size=1):
        assert(all(x >= 0 for x in shape)), f"Invalid shape: {shape}"
        self.shape = tuple(shape)
        self.ndim = len(shape)
        self.name = name
        self.element_size = 1
        self.array = None

        # check that block shape is compatible with array dimension
        if array != None:
            self.array = array
            assert(array.ndim == self.ndim)
            for i in range(array.ndim):
                assert(array.shape[i] % self.shape[i] == 0)

        self.blocks = self.__get_blocks()
        if zeros:
            for b in self.blocks:
                self.blocks[b][0].data = bytearray(math.prod(self.shape))

    def repartition(self, out_blocks, m, get_read_write_blocks):
        '''
        Fills partition out_blocks with data from self.
        out_blocks: a partition. The blocks of this partition will be written
        '''
        read_blocks, write_blocks = get_read_write_blocks(self,
                                                          out_blocks, m)
        cache = Cache(write_blocks, out_blocks)
        for read_block in read_blocks.blocks:
            self.read(read_blocks.blocks[read_block][0])
            blocks = cache.insert(read_blocks.blocks[read_block][0])
            for b in blocks:
                out_blocks.write(b)
                del(b.data) # not sure if this deletes data in read blocks

    def read(self, block):
        '''
        Read block from partition. Shape of block may not match shape of 
        partition.
        '''
        if block.shape == self.shape:
            # Return partition block
            my_block = self.blocks[block.origin][0]
            my_block.read()
            block.data = my_block.data
        else:
            for b in self.blocks:
                block.read_from(self.blocks[b][0])
    
    def write(self, block):
        '''
        Write data in block to partition blocks. Shape of block may not match
        shape of partition.
        '''
        if block.shape == self.shape:
            # Return partition block
            my_block = self.blocks[block.origin][0]
            my_block.data = block.data
            my_block.write()
        else:
            for b in self.blocks:
                block.write_to(self.blocks[b][0])

    def covered_blocks(self, block):
        '''
        Return partition blocks that have a non-empty intersection with block
        '''
        return []

    def __get_blocks(self):
        '''
        Return the list of blocks in this partition
        '''

        # The partition is the array itself
        if self.array == None:
            return [ Block((0, 0, 0), self.shape)]

        blocks = {}
        shape = self.array.shape
        offset = 0
        # This could be a list expansion
        for i in range(int(shape[0]/self.shape[0])):
            for j in range(int(shape[1]/self.shape[1])):
                for k in range(int(shape[2]/self.shape[2])):
                    origin = (i*self.shape[0], j*self.shape[1], k*self.shape[2])
                    blocks[origin] = [ Block(origin, self.shape, element_size=self.element_size, file_name=f'{self.name}_block_{offset}.bin') ]
                    offset += math.prod(self.shape)*self.element_size
        return blocks

    def get_neighbor_block_ind(self, block_ind, dim):
        '''
        Return the index of the neighbor block of block index block_ind along dimension dim, in positive orientation.
        '''
        array_shape = self.array.shape
        n_blocks = [ int(array_shape[i]/self.shape[i]) for i in range(len(self.shape))]
        if dim == 2:
            neighbor_ind = block_ind + 1
        if dim == 1:
            neighbor_ind = block_ind + n_blocks[2]
        if dim == 0:
            neighbor_ind = block_ind + n_blocks[2]*n_blocks[1]
        return neighbor_ind

    def __str__(self):
        if self.array is None:
            return f'Partition of shape {self.shape}'
        else:
            return f'Partition of shape {self.shape} of array of shape {self.array.shape}'

class Block():
    '''
    A block of a partition.

    Attributes:
        origin: the origin of the block. Example: (10, 5, 10)
        shape: the block shape. Example: (5, 10, 5)
    '''
    def __init__(self, origin, shape, data=None, element_size=1, file_name=None):
        assert(all(x >= 0 for x in shape)), f"Invalid shape: {shape}"
        self.origin = tuple(origin)
        self.shape = tuple(shape)
        self.data = data
        self.file_name = file_name
        self.element_size = element_size

    def empty(self):
        '''
        Return True if the block volume is 0
        '''
        return any(x<=0 for x in self.shape)

    def read(self):
        '''
        Read the block from file_name. File file_name has to contain the block 
        and only the block
        '''
        if self.data == None:
            with open(self.file_name, 'rb') as f:
                self.data = f.read()

    def write(self):
        '''
        Write the block to disk.
        '''
        assert(len(self.data) == math.prod(self.shape)*self.element_size)
        with(open(self.file_name), 'wb+') as f:
            f.write(self.data)
        f.close()

    def offset(self, point):
        '''
        Return offset of point in block
        '''
        assert(self.inside(point)), f'Cannot get offset of point {point} which is outside of block {self}'
        return point[2]-self.origin[2] + self.shape[2]*(point[1]-self.origin[1]) + self.shape[2]*self.shape[1]*(point[0]-self.origin[0])

    def get_read_offsets(self, block):
        origin = tuple(max(block.origin[i], self.origin[i]) for i in (0, 1, 2))
        end = tuple(min(block.origin[i] + block.shape[i], self.origin[i] + self.shape[i]) for i in (0, 1, 2))
        if any(end[i] - origin[i] <= 0 for i in (0, 1, 2)):  # blocks don't overlap
            return (), (), ()

        read_points = tuple(((i, j, k), self.offset((i,j,k))) for i in range(origin[0], end[0]) for j in range(origin[1], end[1]) for k in (origin[2], end[2]))
        read_points = tuple(x for i, x in enumerate(read_points) if i==len(read_points)-1 or i==0 or (x[1] != read_points[i+1][1] and x[1] != read_points[i-1][1]))
        return origin, tuple(end[i]-origin[i] for i in (0, 1, 2)), read_points

    def read_from(self, block):
        '''
        Read relevant data sections from block
        '''
        if self.data is None:
            self.data = bytearray(math.prod(self.shape))

        data = bytearray()
        origin, shape, read_points = self.get_read_offsets(block) # empty if blocks don't overlap
        # Read in block
        with open(block.file_name, 'rb') as f:
            for i, r in enumerate(read_points[::2]):
                f.seek(read_points[i][1])
                data += f.read(read_points[i+1][1]-read_points[i][1])
        # Create block from data
        data_block = Block(origin, shape, data)
        _, _, read_points = data_block.get_read_offsets(self)
        offset = 0
        for i, x in enumerate(read_points[::2]):
            self.data[read_points[i][1]:read_points[i+1][1]] = data_block.data[offset:offset+read_points[i+1][1]-read_points[i][1]]
            offset += read_points[i+1][1]-read_points[i][1]

    def write_to(self, block):
        '''
        Write relevant data sections to block
        '''
        data = bytearray()
        _, _, read_points = block.get_read_offsets(self)
        # Read through current block
        for i, x in enumerate(read_points[::2]):
            data += self.data[read_points[i][1]:(read_points[i+1][1])]

        # Create block from data
        origin, shape, read_points = self.get_read_offsets(block)
        offset = 0
        if len(read_points) == 0:
            return
        with open(block.file_name, 'ab+') as f:
            n = int(len(read_points)/2)
            print(f'Writing to {block.file_name} ({n} seeks)')
            b = 0
            for i, r in enumerate(read_points[::2]):
                f.seek(read_points[i][1])
                b += f.write(data[offset:offset+read_points[i+1][1]-read_points[i][1]])
                offset += read_points[i+1][1]-read_points[i][1]
            print(f'Wrote {b} bytes')
        f.close()

    def inside(self, point):
        '''
        Return True if point is inside of self
        '''
        return all(point[i] >= self.origin[i] and point[i]-self.origin[i] <= self.shape[i] for i in (0, 1, 2)) 

    def fill_with_zeros(self):
        '''
        Fill data with zeros
        '''
        self.data = bytearray(math.prod(self.shape))

    def __str__(self):
        return f'Block: origin {self.origin}; shape {self.shape}'

class Cache():

    def __init__(self, write_blocks, out_blocks):
        self.out_blocks = out_blocks
        self.write_blocks = write_blocks

    def insert(self, read_block):
        self.write_blocks.blocks[read_block.origin][0].data = read_block.data # assume read and write blocks are identical
        return [ self.write_blocks.blocks[read_block.origin][0] ] # return the list of write blocks that are ready to be written

'''
    Keep and baseline algorithms
'''

def keep(in_blocks, out_blocks, m):
    r_hat = r_hat(in_blocks, out_blocks)
    read_shapes = candidate_read_shapes(in_blocks, out_blocks, r_hat)
    min_seeks = None
    for r in read_shapes:
        read_blocks = Partition(shape, A)
        write_blocks = create_write_blocks(read_blocks, out_blocks)
        mc = peak_memory(r, write_blocks)
        if mc > m:
            continue
        if r == r_hat:
            best_read_blocks = read_blocks
            break
        s = generated_seeks(in_blocks, read_blocks, write_blocks, out_blocks)
        if min_seeks == None or s < min_seeks:
            min_seeks, best_read_blocks = s, read_blocks
    return best_read_blocks, write_blocks

def baseline(in_blocks, out_blocks, m):
    return in_blocks, in_blocks

'''
    Utils
'''
def r_hat(in_blocks, out_blocks):
    return tuple([ in_blocks.shape[i]*(1 + 
                                       math.floor(out_blocks.shape[i]/in_blocks.shape[i]))
                                       for i in range(in_blocks.ndim)])


def candidate_read_shapes(in_blocks, out_blocks, r_hat, array):
    assert(in_blocks.ndim == 3), 'Only supports dimension 3'
    divs0 = [x for x in divisors(array.shape[0]) if x <= r_hat[0]]
    divs1 = [x for x in divisors(array.shape[1]) if x <= r_hat[1]]
    shapes = [ (x, y, r_hat[2]) for x in divs0 for y in divs1 ]
    shapes.sort(key=lambda x: (x[1], x[0]),reverse=True)
    return shapes

def divisors(n):
    return [x for x in range(1, int(n/2+1)) if n % x == 0]

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


def create_write_blocks(read_blocks, out_blocks):
    '''
        read_block: partition
        out_blocks: partition
    '''
    f_blocks = [ get_F_blocks(r, out_blocks) for r in read_blocks.blocks ]
    write_blocks = {}

    # Move F blocks to be merged
    for i in range(len(f_blocks)):
        # Don't touch F0
        write_blocks[f_blocks[i][0].origin] = [ (f_blocks[i][0], None) ] # key: origin, value: (shape, data)
        for f in range(1, 8):
            if not f_blocks[i][f] is None:
                destF0 = destination_F0(read_blocks, i, f)
                print(f'Block {i}: moving F{f} to Block {destF0} F0')
                write_blocks[f_blocks[i][0].origin] += [ f_blocks[i][f] ]
    
    return write_blocks


def get_F_blocks(write_block, out_blocks):
    '''
    Assuming out_blocks are of uniform size
    '''

    # F0 is where the write_block origin is
    origin = write_block.origin
    out_ends = [ math.floor((write_block.origin[i]+write_block.shape[i])/out_blocks.shape[i])*out_blocks.shape[i] for i in range(len(origin)) ]
    shape = [ out_ends[i]-write_block.origin[i] if out_ends[i] > write_block.origin[i] else write_block.shape[i] for i in range(len(origin))] 
    F0 = Block(origin, shape)

    # F1
    origin = (F0.origin[0], F0.origin[1], F0.origin[2] + F0.shape[2])
    shape = [ F0.shape[0], F0.shape[1], write_block.shape[2] - F0.shape[2] ]
    F1 = Block(origin, shape)

    # F2
    origin = (F0.origin[0], F0.origin[1] + F0.shape[1], F0.origin[2])
    shape = [ F0.shape[0], write_block.shape[1] - F0.shape[1], F0.shape[2] ]
    F2 = Block(origin, shape)

    # F3
    origin = (F0.origin[0], F0.origin[1] + F0.shape[1], F0.origin[2] + F0.shape[2])
    shape = [ F0.shape[0], write_block.shape[1] - F0.shape[1], write_block.shape[2] - F0.shape[2] ]
    F3 = Block(origin, shape)

    # F4
    origin = ( F0.origin[0] + F0.shape[0], F0.origin[1], F0.origin[2] )
    shape = [ write_block.shape[0] - F0.shape[0], F0.shape[1], F0.shape[2] ]
    F4 = Block(origin, shape)

    # F5
    origin = ( F0.origin[0] + F0.shape[0], F0.origin[1], F0.origin[2] + F0.shape[2] )
    shape = [ write_block.shape[0] - F0.shape[0], F0.shape[1], write_block.shape[2] - F0.shape[2] ]
    F5 = Block(origin, shape)

    # F6
    origin = ( F0.origin[0] + F0.shape[0], F0.origin[1] + F0.shape[1], F0.origin[2] )
    shape = [ write_block.shape[0] - F0.shape[0], write_block.shape[1] - F0.shape[1], F0.shape[2] ]
    F6 = Block(origin, shape)

    # F7
    origin = ( F0.origin[0] + F0.shape[0], F0.origin[1] + F0.shape[1], F0.origin[2] + F0.shape[2] )
    shape = [ write_block.shape[0] - F0.shape[0], write_block.shape[1] - F0.shape[1], write_block.shape[2] - F0.shape[2] ]
    F7 = Block(origin, shape)

    # Remove empty blocks and return
    f_blocks = [ f if not f.empty() else None for f in [F0, F1, F2, F3, F4, F5, F6, F7]]
    return f_blocks