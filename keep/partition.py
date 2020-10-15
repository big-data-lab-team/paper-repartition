import math
import os
from block import Block
from cache import Cache


class Partition():
    '''
    A uniform partition of the array to be repartitioned. May be the array
    itself (partition of size 1), the input, output, read or write blocks.

    Attributes:
        shape: the shape of the blocks in the partition
        name: the name of the partition, from which block
              file names are generated
        array: a partition with 1 block, representing the partitioned array
              (might be None)
        blocks: a dictionary representing the blocks in the partition.
                Key is the block origin, value is the Block object.
        fill: 'zeros' to fill the block buffers with zeros, 'random' to fill
              them with random data, None to not fill them at all.
              Warning: this allocates memory.
        create_blocks: if set to False, don't create the blocks in the
              partition.
    '''

    def __init__(self, shape, name, array=None, fill=None, create_blocks=True):
        '''
        Constructor
        '''
        assert(all(x >= 0 for x in shape)), f"Invalid shape: {shape}"
        self.shape = tuple(shape)
        self.ndim = len(shape)
        self.name = name
        self.array = self

        # check that block shape is compatible with array dimension
        if array is not None:
            self.array = array
            assert(array.ndim == self.ndim)
            assert(all(array.shape[i] % self.shape[i] == 0
                       for i in range(array.ndim)))

        if create_blocks:
            self.blocks = self.__get_blocks(fill)

    def __get_blocks(self, fill):
        '''
        Initialize and return the list of blocks in this partition

        Arguments:
            fill: argument passed to Block
        '''

        # Blocks have to be created
        blocks = {}
        shape = self.array.shape
        # Warning: read order of blocks in repartition
        # depends on this key order...
        ni = int(shape[0]/self.shape[0])
        nj = int(shape[1]/self.shape[1])
        nk = int(shape[2]/self.shape[2])
        size = math.prod(self.shape)
        blocks = {(i*self.shape[0], j*self.shape[1], k*self.shape[2]):
                  Block((i*self.shape[0], j*self.shape[1], k*self.shape[2]),
                        self.shape, fill=fill,
                        file_name=(f'{self.name}_block_'
                                   f'{size*(k+j*nk+i*nj*nk)}.bin'))
                  for i in range(ni)
                  for j in range(nj)
                  for k in range(nk)}
        return blocks

    def __str__(self):
        '''
        Return a string representation for the partition
        '''
        blocks = os.linesep.join([str(self.blocks[b]) for b in self.blocks])

        return (f'Partition of shape {self.shape} of array of shape '
                f'{self.array.shape}. Blocks:' + os.linesep + blocks)

    def clear(self):
        '''
        Clear all the blocks in the partition
        '''
        for b in self.blocks:
            self.blocks[b].clear()

    def get_neighbor_block_ind(self, block_ind, dim):
        '''
        Return the block index of the neighbor of block of index block_ind
        along dimension dim, in positive orientation.

        Arguments:
            block_ind: index of a block in the partition
            dim: 0, 1 or 2 (3D partition)
        '''
        array_shape = self.array.shape
        n_blocks = [int(array_shape[i]/self.shape[i])
                    for i in range(len(self.shape))]
        if dim == 2:
            neighbor_ind = block_ind + 1
        if dim == 1:
            neighbor_ind = block_ind + n_blocks[2]
        if dim == 0:
            neighbor_ind = block_ind + n_blocks[2]*n_blocks[1]
        return neighbor_ind

    def read_block(self, block):
        '''
        Read block from partition. Shape of block may or may not match shape of
        partition.

        Similar to write_block but for reads
        '''
        seeks = 0
        total_bytes = 0
        for b in self.blocks:
            # block may be read from multiple blocks of self
            t, s = block.read_from(self.blocks[b])
            seeks += s
            total_bytes += t
        return total_bytes, seeks

    def repartition(self, out_blocks, m, get_read_blocks_and_cache):
        '''
        Write data from self in files of partition out_blocks. Implements
        Algorithm 1 in the paper.

        Arguments:
            out_blocks: a partition. The blocks of this partition are written.
            m: memory constraint.
            get_read_blocks_and_cache: function that returns read blocks and
                                       an initialized cache from
                                       (in_blocks, out_blocks, m, array)

        Return number of bytes read or written, and number of seeks done
        '''
        log('')
        log(f'repartition: # Repartitioning {self.name} in {out_blocks.name}')
        r, c, e = get_read_blocks_and_cache(self, out_blocks, m, self.array)
        read_blocks, cache, expected_seeks = (r, c, e)
        #log(f'repartition: Selected read blocks: {read_blocks}')
        #log(f'repartition: Cache: {cache}')
        seeks = 0
        total_bytes = 0
        for read_block in read_blocks.blocks:
            t, s = self.read_block(read_blocks.blocks[read_block])
            log(f'repartition: Read required {s} seeks')
            total_bytes += t
            seeks += s
            complete_blocks = cache.insert(read_blocks.blocks[read_block])
            for b in complete_blocks:
                log(f'repartition: Writing complete block {b}')
                # TODO: it's a bit overkill to write_to all the output blocks
                # although nothing is actually written to blocks that don't
                # need it. Also, write_to is not well named,
                # it is the block being written to the partition
                t, s = out_blocks.write_block(b)
                log(f'repartition: Write required {s} seeks')
                total_bytes += t
                seeks += s
                b.clear()
        message = f'Incorrect seek count. Expected: {expected_seeks}. Real: {seeks}'
        assert(expected_seeks == seeks), message
        return total_bytes, seeks

    def write(self):
        '''
        Write all the partition blocks to file.
        '''
        for b in self.blocks:
            self.blocks[b].write()

    def write_block(self, block):
        '''
        Write data in block to partition blocks. Shape of block may not match
        shape of partition.

        Similar to read_block but for writes
        '''
        seeks = 0
        total_bytes = 0
        if block.shape == self.shape:
            # Return partition block
            my_block = self.blocks[block.origin]
            my_block.data = block.data
            total_bytes = my_block.write()
            seeks = 1
        else:
            for b in self.blocks:
                # block may be written to multiple blocks in self
                t, s = block.write_to(self.blocks[b])
                seeks += s
                total_bytes += t
        return total_bytes, seeks


def log(message, level=0):
    '''
    Temporary logger
    '''
    LOG_LEVEL = 1
    if level >= LOG_LEVEL:
        print(message)
