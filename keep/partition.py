
import math
import os
from block import Block
from cache import Cache
from log import log


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

    def delete(self):
        '''
        Delete all the blocks in the partition from disk
        '''
        for b in self.blocks:
            self.blocks[b].delete()

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

    def read_block(self, block, dry_run=False):
        '''
        Read block from partition. Shape of block may or may not match shape of
        partition.

        Similar to write_block but for reads
        '''
        seeks = 0
        total_bytes = 0
        read_time = 0
        for b in self.blocks:
            if not self.blocks[b].overlap(block):
                continue
            # block may be read from multiple blocks of self
            t, s, rt = block.read_from(self.blocks[b], dry_run)
            seeks += s
            total_bytes += t
            read_time += rt
        return total_bytes, seeks, read_time

    def repartition(self, out_blocks, m, get_read_blocks_and_cache,
                    dry_run=False):
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
        r, c, e, p = get_read_blocks_and_cache(self, out_blocks, m, self.array)
        read_blocks, cache, expected_seeks, est_peak_mem = (r, c, e, p)
        seeks = 0
        peak_mem = 0
        total_bytes = 0
        bytes_in_cache = 0
        read_time = 0
        write_time = 0
        for read_block in read_blocks.blocks:
            log(f'repartition: reading block: {read_block}', 0)
            t, s, rt = self.read_block(read_blocks.blocks[read_block], dry_run)
            bytes_in_cache += t
            total_bytes += t
            seeks += s
            read_time += rt
            log(f'repartition: inserting read block of size '
                f'{read_blocks.blocks[read_block].mem_usage()}B to cache')
            complete_blocks = cache.insert(read_blocks.blocks[read_block],
                                           dry_run)
            log(f'repartition: Cache: {str(cache)}', 0)
            peak_mem = max(peak_mem, cache.mem_usage())
            for b in complete_blocks:
                log(f'repartition: Writing complete block {b}', 0)
                t, s, wt = out_blocks.write_block(b, dry_run)
                assert(t == b.mem_usage())
                b.clear()
                bytes_in_cache -= t
                log(f'repartition: Write required {s} seeks', 0)
                log(f'repartition: Cache: {str(cache)}', 0)
                total_bytes += t
                seeks += s
                write_time += wt
                b.clear()
            message = (f'{bytes_in_cache}, {cache.mem_usage()}')
            assert(bytes_in_cache == cache.mem_usage()), message

        message = (f'Incorrect seek count. Expected: {expected_seeks}.'
                   f' Real: {seeks}')
        assert(dry_run or (expected_seeks == seeks)), message
        message = (f'Incorrect memory usage. Expected: {est_peak_mem}B.'
                   f' Real: {peak_mem}B.')
      #  assert(dry_run or (est_peak_mem == peak_mem)), message
        return total_bytes, seeks, peak_mem, read_time, write_time

    def write(self):
        '''
        Write all the partition blocks to file.
        '''
        for b in self.blocks:
            self.blocks[b].write()

    def write_block(self, block, dry_run=False):
        '''
        Write data in block to partition blocks. Shape of block may not match
        shape of partition.

        Similar to read_block but for writes
        '''
        seeks = 0
        total_bytes = 0
        write_time = 0
        #print(f'write {block} to {self} with {len(self.blocks)} calls')
        for b in self.blocks:
            # block may be written to multiple blocks in self
            if not self.blocks[b].overlap(block):
                continue
            t, s, wt = block.write_to(self.blocks[b], dry_run)
            seeks += s
            total_bytes += t
            write_time += wt
        return total_bytes, seeks, write_time
