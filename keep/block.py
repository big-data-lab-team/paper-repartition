import math
import os

class Block():
    '''
    A block of a partition.

    Attributes:
        origin: the origin of the block. Example: (10, 5, 10)
        shape: the block shape. Example: (5, 10, 5)
    '''
    def __init__(self, origin, shape, data=None, element_size=1, file_name=None, fill=None):
        assert(len(shape) >= 1), f'Invalid shape: {shape}'
        assert(all(x >= 0 for x in shape)), f"Invalid shape: {shape}"
        assert(data is None or fill is None), "Cannot set both block data and fill pattern"
        assert(len(origin) == len(shape)), f"Origin {origin} and shape {shape} don't match"
        self.origin = tuple(origin)
        self.shape = tuple(shape)
        self.data = data
        self.file_name = file_name
        self.element_size = element_size
        if fill=='zeros':
            self.data = bytearray(math.prod(self.shape))
        if fill=='random':
            self.data = bytearray(os.urandom(math.prod(self.shape)))

    def empty(self):
        '''
        Return True if the block volume is 0
        '''
        return any(x<=0 for x in self.shape)

    def clear(self):
        self.data = None

    def read(self):
        '''
        Read the block from file_name. File file_name has to contain the block 
        and only the block
        '''
        log(f'<< Reading {self.file_name}', 0)
        with open(self.file_name, 'rb') as f:
            self.data = f.read()
        return len(self.data)

    def write(self):
        '''
        Write the block to disk.
        '''
        assert(self.data is not None), 'Cannot write block with no data'
        assert(len(self.data) == math.prod(self.shape)*self.element_size), "Block shape doesn't match data size"
        with open(self.file_name, 'wb+') as f:
            b = f.write(self.data)
        f.close()
        return b

    def offset(self, point):
        '''
        Return offset of point in self
        '''
        assert(self.inside(point)), f'Cannot get offset of point {point} which is outside of block {self}'
        offset = point[2]-self.origin[2] + self.shape[2]*(point[1]-self.origin[1]) + self.shape[2]*self.shape[1]*(point[0]-self.origin[0])
        return offset
    
    def complete(self):
        '''
        Return True if block contains all the data it's supposed to
        '''
        return len(self.data) == math.prod(self.shape)

    def block_offsets(self, block):
        '''
        Return the offsets in self of contiguous data segments in block.
        '''
        origin = tuple(max(block.origin[i], self.origin[i]) for i in (0, 1, 2))
        end = tuple(min(block.origin[i] + block.shape[i], self.origin[i] + self.shape[i]) for i in (0, 1, 2))
        if any(end[i] - origin[i] <= 0 for i in (0, 1, 2)):  # blocks don't overlap
            return (), (), ()

        read_points = tuple(((i, j, k),
                            self.offset((i,j,k))) for i in range(origin[0], end[0])
                                                  for j in range(origin[1], end[1])
                                                  for k in (origin[2], end[2]-1))
        # Remove duplicate offsets
        read_points = tuple(x for i, x in enumerate(read_points)
                            if i == len(read_points)-1 or i ==0 or # always keep the first and last
                            ((i % 2 == 0 or x[1] != read_points[i+1][1]-1) and
                             (i % 2 == 1 or x[1] != read_points[i-1][1]+1)
                            )
                            )
        # TODO: should there be a +1 in the shape? think end=origin. 
        # Don't think so, end already has the + 1 (works well with ranges so don't change it)
        # Also, removing the +1 crashes the content tests
        return origin, tuple(end[i]-origin[i] for i in (0, 1, 2)), read_points

    def read_from(self, block):
        '''
        Read relevant data sections from block
        '''
        if self.data is None:
            self.data = bytearray(math.prod(self.shape))
        data = bytearray()
        origin, shape, block_offsets = block.block_offsets(self) # empty if blocks don't overlap
        if len(block_offsets) == 0: 
            return 0, 0 # nothing to read
        # Read in block
        seeks = len(block_offsets)/2
        with open(block.file_name, 'rb') as f:
            log(f'<< Reading from {block.file_name} ({len(block_offsets)/2} seeks)', 0)
            total_bytes = 0
            for i, r in enumerate(block_offsets):
                if i % 2 == 1:
                    continue
                log(f'Seek to offset {block_offsets[i][1]} in {block.file_name}')
                f.seek(block_offsets[i][1])
                data += f.read(block_offsets[i+1][1]-block_offsets[i][1] + 1)
                total_bytes += block_offsets[i+1][1]-block_offsets[i][1] + 1
            assert(len(data) == total_bytes)
            log(f'Read {total_bytes} bytes', 0)

        # Write data block to self
        data_block = Block(origin=origin, shape=shape, data=data)
        self.put_data_block(data_block)

        return total_bytes, seeks

    def put_data_block(self, block):
        '''
        Write block.data into self.data
        '''
#        assert(len(self.data) + len(block.data) <= math.prod(self.shape)), 'No space available in block'
        _, _, self_offsets = self.block_offsets(block)
        data_offset = 0
        for i, x in enumerate(self_offsets):
            if i % 2 == 1:
                continue
            next_data_offset = data_offset + self_offsets[i+1][1] - self_offsets[i][1] + 1
            self.data[self_offsets[i][1]:self_offsets[i+1][1]+1] = block.data[data_offset:next_data_offset]
            data_offset = next_data_offset

    def get_data_block(self, block):
        '''
        Assemble and return the block of data from self that matches 
        the geometry of block
        '''

        data = bytearray()
        origin, shape, self_offsets = self.block_offsets(block)
        log(f'Self offsets: {self_offsets}')
        if len(self_offsets) == 0: # if there is nothing to read there is nothing to write
            return Block((-1, -1, -1), (0, 0, 0))
        for i, x in enumerate(self_offsets):
            if i % 2 == 1:
                continue
            data += self.data[self_offsets[i][1]:(self_offsets[i+1][1]+1)]
        
        # Data is now the continuous block of data from self to be written into block
        # TODO: check that no data was actually copied
        return Block(origin, shape, data=data)

    def write_to(self, block):
        '''
        Write relevant data sections to block
        '''
        assert(block.file_name), f"Block {block} has no file name"
        log(f'Writing block {self} to {block}')

        data_b = self.get_data_block(block)
        data = data_b.data

        _, _, block_offsets = block.block_offsets(data_b)
        log(f'Block offsets: {block_offsets}')
        # block offsets are now the offsets in the block to be written
        
        data_offset = 0
        seeks = len(block_offsets) / 2
        mode = 'wb'
        if os.path.exists(block.file_name):  # if file already exists, open in r+b mode to modify without overwriting
            mode = 'r+b'
        with open(block.file_name, mode) as f:
            log(f'>> Writing to {block.file_name} ({len(block_offsets)/2} seeks)', 0)
            total_bytes = 0
            log(block_offsets, 0)
            for i, r in enumerate(block_offsets):
                if i % 2 == 1:
                    continue
                log(f'Seek to offset {block_offsets[i][1]} in {block.file_name}')
                f.seek(block_offsets[i][1])
                log(f'Current position in file: {f.tell()}')
                next_data_offset = data_offset+block_offsets[i+1][1]-block_offsets[i][1]+1
                log(f'Write data from {data_offset} to {next_data_offset}'
                    f' ({data[data_offset:next_data_offset]})')
                wrote_bytes = f.write(data[data_offset:next_data_offset])
                log(f'Wrote {wrote_bytes} bytes')
                log(f'Current position in file: {f.tell()}')
                total_bytes += wrote_bytes
                data_offset = next_data_offset
            log(f'Wrote {total_bytes} bytes in total', 0)
        f.close()
        return total_bytes, seeks

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
        s = 0
        if hasattr(self, 'data'):
            if self.data != None:
                s = len(self.data)
        
        desc = f'Block: origin {self.origin}; shape {self.shape}; data in mem: {s}B'
        if self.file_name != None:
            desc += f'; file_name: {self.file_name}'
        return desc

def log(message, level=0):
    LOG_LEVEL=1
    if level >= LOG_LEVEL:
        print(message)