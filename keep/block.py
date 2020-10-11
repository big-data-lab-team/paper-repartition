import math
import os


class Data():
    '''
    The data buffer stored by a Block. The implementation uses bytearrays 
    but it might be changed to numpy views in the future, to reduce memory
    consumption.
    '''
    def __init__(self, data=None):
        '''
        Default constructor

        Optional keyword arguments:
        data: a bytearray containing the data to put in the buffer
        '''
        if data is None:
            self.data = bytearray()
        else:
            self.data = data

    def mem_usage(self):
        '''
        Return the size of the data buffer in memory
        '''
        return len(self.data)

    def clear(self):
        '''
        Clear the buffer content
        '''
        self.data = bytearray()

    def put(self, offset, buffer):
        '''
        Insert buffer at given offset in self. This function was the main 
        motivation to create the class
        '''
        if len(self.data) < offset:
            # We need to allocate the buffer until the offset
            # TODO: This increases mem usage, might be solved by numpy views
            self.data[len(self.data):offset] = bytearray(offset-len(self.data))

        self.data[offset:offset+len(buffer)] = buffer
        assert(self.data[offset:offset+len(buffer)] == buffer)

    def get(self, start_offset, end_offset):
        '''
        Returns the data between start_offset (included) 
        and end_offset (excluded)
        '''
        buffer = self.data[start_offset:end_offset]
        return buffer

    def bytes(self):
        '''
        Return a bytearray containing the data in the buffer
        '''
        return self.data


class Block():
    '''
    A block of a partition.

    '''
    def __init__(self, origin, shape, data=None, file_name=None, fill=None):
        '''
        Attributes:
            origin: the origin of the block. Example: (10, 5, 10)
            shape: the block shape. Example: (5, 10, 5)
            data: bytearray to initialize the data buffer
            file_name: file name where to read and write the block
            fill: the pattern to initialize the data buffer: 'zeros' or 'random'
        '''
        assert(len(shape) >= 1), f'Invalid shape: {shape}'
        assert(all(x >= 0 for x in shape)), f"Invalid shape: {shape}"
        assert(data is None or fill is None), (f"Cannot set both block data"
                                               " and fill pattern: "
                                               " {len(data)},"
                                               " {fill}")
        assert(len(origin) == len(shape)), (f"Origin {origin} and shape "
                                            "{shape} don't match")
        self.origin = tuple(origin)
        self.shape = tuple(shape)
        self.file_name = file_name

        # Create data buffer
        if fill == 'zeros':
            data = bytearray(math.prod(self.shape))
        if fill == 'random':
            data = bytearray(os.urandom(math.prod(self.shape)))
        if data is None:
            data = bytearray()
        self.data = Data(data)

    def block_offsets(self, block):
        '''
        Return the offsets in self of contiguous data segments in block.
        '''
        origin = tuple(max(block.origin[i], self.origin[i]) for i in (0, 1, 2))
        end = tuple(min(block.origin[i] + block.shape[i],
                    self.origin[i] + self.shape[i]) for i in (0, 1, 2))
        if any(end[i] - origin[i] <= 0 for i in (0, 1, 2)):
            # blocks don't overlap
            return (), (), ()

        read_points = tuple(((i, j, k),
                            self.offset((i, j, k)))
                            for i in range(origin[0], end[0])
                            for j in range(origin[1], end[1])
                            for k in (origin[2], end[2]-1))
        # Remove duplicate offsets
        read_points = tuple(x for i, x in enumerate(read_points)
                            # always keep the first and last
                            if i == len(read_points)-1 or i == 0 or
                            ((i % 2 == 0 or x[1] != read_points[i+1][1]-1) and
                             (i % 2 == 1 or x[1] != read_points[i-1][1]+1))
                            )
        return origin, tuple(end[i]-origin[i] for i in (0, 1, 2)), read_points

    def clear(self):
        '''
        Clear the data buffer in the block
        '''
        self.data.clear()

    def complete(self):
        '''
        Return True if buffer contains data for the entire
        region covered by block
        '''
        return self.data.mem_usage() == math.prod(self.shape)

    def empty(self):
        '''
        Return True if the block volume is 0
        '''
        return any(x <= 0 for x in self.shape)

    def get_data_block(self, block):
        '''
        Assemble and return the block of data from self that intersects
        with block

        Return a Block object containing the said block of data

        Similar to put_data_block but to copy from self to block
        '''

        data = bytearray()
        origin, shape, self_offsets = self.block_offsets(block)
        if len(self_offsets) == 0:
            # if there is nothing to read there is nothing to write
            return Block((-1, -1, -1), (0, 0, 0))
        for i, x in enumerate(self_offsets):
            if i % 2 == 1:
                continue
            data += self.data.get(self_offsets[i][1], (self_offsets[i+1][1]+1))

        # Data is now the continuous block of data
        # from self to be written into block
        # TODO: check that no data was actually copied
        return Block(origin, shape, data=data)

    def inside(self, point):
        '''
        Return True if point is inside of self
        '''
        return all(point[i] >= self.origin[i] and
                   point[i]-self.origin[i] <= self.shape[i]
                   for i in (0, 1, 2))

    def offset(self, point):
        '''
        Return offset of point in self
        '''
        assert(self.inside(point)), (f'Cannot get offset of point {point}'
                                     ' which is outside of block {self}')
        offset = (point[2]-self.origin[2] +
                  self.shape[2]*(point[1]-self.origin[1]) +
                  self.shape[2]*self.shape[1]*(point[0]-self.origin[0]))
        return offset

    def put_data_block(self, block):
        '''
        Write the relevant sections of block.data into self.data

        Similar to get_data_block but to copy from block to self
        '''
        _, _, self_offsets = self.block_offsets(block)
        data_offset = 0
        for i, x in enumerate(self_offsets):
            if i % 2 == 1:
                continue
            next_data_offset = (data_offset + self_offsets[i+1][1] -
                                self_offsets[i][1] + 1)
            self.data.put(self_offsets[i][1], block.data.get(data_offset,
                                                             next_data_offset))
            data_offset = next_data_offset
        message = (f'Block is {block.data.mem_usage()}B but only {data_offset}'
                   ' were copied')
        assert(data_offset == block.data.mem_usage()), message

    def read(self):
        '''
        Read the block from argument file_name. File file_name has to contain
        the block and only the block

        Return number of bytes read

        Similar to write but for reading
        '''
        log(f'<< Reading {self.file_name}', 0)
        with open(self.file_name, 'rb') as f:
            self.data.put(0, f.read())
        assert(self.data.mem_usage() == math.prod(self.shape))
        return self.data.mem_usage()

    def read_from(self, block):
        '''
        Read the relevant data sections of self from block's file name.
        In general, block doesn't have the same origin or shape as self.

        Return: (total_bytes, seeks), the total number of bytes read and the
        number of seeks required in block.

        Similar to write_to but for reading
        '''

        data = bytearray()
        origin, shape, block_offsets = block.block_offsets(self)
        if len(block_offsets) == 0:
            return 0, 0  # nothing to read
        # Read in block
        seeks = len(block_offsets)/2
        with open(block.file_name, 'rb') as f:
            log(f'<< Reading from {block.file_name}'
                f' ({len(block_offsets)/2} seeks)', 0)
            total_bytes = 0
            for i, r in enumerate(block_offsets):
                if i % 2 == 1:
                    continue
                f.seek(block_offsets[i][1])
                data += f.read(block_offsets[i+1][1]-block_offsets[i][1] + 1)
                total_bytes += block_offsets[i+1][1]-block_offsets[i][1] + 1
            assert(len(data) == total_bytes), (f'Data size: {len(data)}, '
                                               'read {total_bytes} bytes '
                                               ' from block {block}')
            log(f'Read {total_bytes} bytes', 0)

        # Write data block to self
        data_block = Block(origin=origin, shape=shape, data=data)
        self.put_data_block(data_block)

        return total_bytes, seeks

    def write(self):
        '''
        Write the block to the file name in argument file_name. Block has to
        be complete.

        Return the number of bytes written to file
        '''
        assert(self.data.mem_usage() > 0), 'Cannot write block with no data'
        assert(self.data.mem_usage() ==
               math.prod(self.shape)), ("Block shape"
                                        " doesn't match data size")
        with open(self.file_name, 'wb+') as f:
            b = f.write(self.data.get(0, math.prod(self.shape)))
        f.close()
        return b

    def write_to(self, block):
        '''
        Write relevant data sections of self to block's file name

        Return: (total_bytes, seeks), the total number of bytes written and the
        number of seeks required in block.

        Similar to read_from but for writes.
        '''
        assert(block.file_name), f"Block {block} has no file name"
        log(f'Writing block {self} to {block}')

        data_b = self.get_data_block(block)
        data = data_b.data

        _, _, block_offsets = block.block_offsets(data_b)
        # block offsets are now the offsets in the block to be written

        data_offset = 0
        seeks = len(block_offsets) / 2
        mode = 'wb'
        if os.path.exists(block.file_name):
            # if file already exists, open in r+b mode
            #  to modify without overwriting
            mode = 'r+b'
        with open(block.file_name, mode) as f:
            log(f'  >> Writing to {block.file_name}'
                ' ({len(block_offsets)/2} seeks)', 0)
            total_bytes = 0
            for i, r in enumerate(block_offsets):
                if i % 2 == 1:
                    continue
                f.seek(block_offsets[i][1])
                next_data_offset = (data_offset +
                                    block_offsets[i+1][1] -
                                    block_offsets[i][1] + 1)
                wrote_bytes = f.write(data.get(data_offset, next_data_offset))
                total_bytes += wrote_bytes
                data_offset = next_data_offset
            log(f'  Wrote {total_bytes} bytes in total', 0)
        f.close()
        return total_bytes, seeks

    def __str__(self):
        '''
        Return a string representation for self
        '''
        s = self.data.mem_usage()
        desc = (f'Block: origin {self.origin}; shape {self.shape};'
                f' data in mem: {s}B')
        if self.file_name is not None:
            desc += f'; file_name: {self.file_name}'
        return desc


def log(message, level=0):
    '''
    Temporary logger
    '''
    LOG_LEVEL = 0
    if level >= LOG_LEVEL:
        print(message)
