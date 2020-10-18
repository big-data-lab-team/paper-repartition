import math
import os


class Data():
    '''
    The data buffer stored by a Block. The implementation uses bytearrays
    but it might be changed to numpy views in the future, to reduce memory
    consumption.
    '''
    def __init__(self, data):
        '''
        Default constructor

        Optional keyword arguments:
        data: a bytearray containing the data to put in the buffer
        '''
        self.data = data
        self.bytes_put = len(data)

    def bytes(self):
        '''
        Return a bytearray containing the data in the buffer
        '''
        return self.data

    def clear(self):
        '''
        Clear the buffer content
        '''
        self.data = bytearray()
        self.bytes_put = 0

    def get(self, start_offset, end_offset):
        '''
        Returns the data between start_offset (included)
        and end_offset (excluded)
        '''
        buffer = self.data[start_offset:end_offset]
        return buffer

    def get_data_size(self):
        '''
        Get the data size of this buffer. For use in dry mode executions.
        '''
        return self.bytes_put

    def mem_usage(self):
        '''
        Return the size of the data buffer in memory
        '''
        # Warning: this is not real memory usage due to Python overheads
        # and most importantly zero padding in put()
        return self.bytes_put

    def put(self, offset, buffer, dry_run=False):
        '''
        Insert buffer at given offset in self. This function was the main
        motivation to create the class
        '''

        self.bytes_put += len(buffer)
        if dry_run:
            return

        if len(self.data) < offset:
            # We need to allocate the buffer until the offset
            # TODO: This increases mem usage, might be solved by numpy views
            self.data[len(self.data):offset] = bytearray(offset-len(self.data))

        self.data[offset:offset+len(buffer)] = buffer
        assert(self.data[offset:offset+len(buffer)] == buffer)

    def set_data_size(self, size):
        '''
        Set the data size of this buffer. For use in dry mode executions.
        '''
        self.bytes_put = size


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
            fill: the pattern to initialize the data buffer: 'zeros' or
                  'random'
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
        self.end = tuple(origin[i] + shape[i] - 1 for i in (0, 1, 2))
        self.file_name = file_name

        # Create data buffer
        if fill == 'zeros':
            data = bytearray(math.prod(self.shape))
        if fill == 'random':
            data = bytearray(os.urandom(math.prod(self.shape)))
        if data is None:
            data = bytearray()
        self.data = Data(data)

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

    def block_offsets(self, block):
        '''
        Return the offsets in self of contiguous data segments of block.
        '''

        # If self and block don't overlap then don't bother
        if not self.overlap(block):
            return (), (), ()

        # Origin and end + 1 of the intersection between self and block
        origin = (max(block.origin[0], self.origin[0]),
                  max(block.origin[1], self.origin[1]),
                  max(block.origin[2], self.origin[2]))
        end = tuple(min(block.origin[i] + block.shape[i],
                    self.origin[i] + self.shape[i]) for i in (0, 1, 2))

        delta_2 = end[2] - origin[2]
        delta_1 = self.shape[2] - (end[2] - origin[2])
        delta_0 = (self.shape[1] - (end[1] - origin[1]))*self.shape[2]

        current_offset = self.offset(origin)
        read_points = []  # start new segment

        start_seg = current_offset
        for i in range(end[0]-origin[0]):
            for j in range(end[1]-origin[1]):
                if delta_2 != 0:
                    current_offset += delta_2  # extend segment
                if delta_1 != 0:
                    end_seg = current_offset - 1
                    read_points += [start_seg, end_seg]
                    current_offset += delta_1
                    start_seg = current_offset
            if delta_0 != 0:
                if delta_1 == 0:
                    end_seg = current_offset - 1
                    read_points += [start_seg, end_seg]
                current_offset += delta_0
                start_seg = current_offset

        end_seg = current_offset - 1
        if read_points == []:
            read_points += [start_seg, end_seg]
        return origin, tuple(end[i]-origin[i] for i in (0, 1, 2)), tuple(read_points)

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

    def get_data_block(self, block, dry_run=False):
        '''
        Assemble and return the block of data from self that intersects
        with block

        Return a Block object containing the said block of data

        Similar to put_data_block but to copy from self to block
        '''

        if not self.overlap(block):
            return Block((-1, -1, -1), (0, 0, 0))

        origin, shape, self_offsets = self.block_offsets(block)

        data_size = sum([(self_offsets[i+1]+1)-self_offsets[i]
                        if i % 2 == 0 else 0
                        for i in range(len(self_offsets))])
        data = bytearray()
        for i, x in enumerate(self_offsets):
            if i % 2 == 1 or dry_run:
                continue
            data += self.data.get(self_offsets[i], (self_offsets[i+1]+1))
        assert(dry_run or (len(data) == data_size)), f'{dry_run}, {len(data)}, {data_size}'
        block = Block(origin, shape, data)
        if dry_run:
            block.set_data_size(data_size)
        # Data is now the continuous block of data
        # from self to be written into block
        # TODO: check that no data was actually copied
        return block

    def get_data_size(self):
        '''
        Get the data size of the data buffer. For use in dry mode executions.
        '''
        return self.data.get_data_size()

    def inside(self, point):
        '''
        Return True if point is inside of self
        '''
        return all(point[i] >= self.origin[i] and
                   point[i]-self.origin[i] <= self.shape[i]
                   for i in (0, 1, 2))

    def mem_usage(self):
        '''
        Return the memory usage of the block
        '''
        return self.data.mem_usage()

    def offset(self, point):
        '''
        Return offset of point in self
        '''
        # This assertion turns out to be very costly
        # assert(self.inside(point)), (f'Cannot get offset of point {point}'
        #                             ' which is outside of block {self}')
        offset = (point[2]-self.origin[2] +
                  self.shape[2]*(point[1]-self.origin[1]) +
                  self.shape[2]*self.shape[1]*(point[0]-self.origin[0]))
        return offset

    def overlap(self, block):
        '''
        Return True if self ovelaps with block
        '''
        return all((block.origin[i] >= self.origin[i] and
                    block.origin[i] <= self.end[i])
                   or (self.origin[i] >= block.origin[i] and
                       self.origin[i] <= block.end[i])
                   for i in (0, 1, 2)
                   )

    def put_data_block(self, block, dry_run=False):
        '''
        Write the relevant sections of block.data into self.data

        Similar to get_data_block but to copy from block to self
        '''
        assert(self.data.mem_usage() <= math.prod(self.shape)), message

        if not self.overlap(block):
            return

        _, _, self_offsets = self.block_offsets(block)
        data_offset = 0
        for i, x in enumerate(self_offsets):
            if i % 2 == 1:
                continue
            next_data_offset = (data_offset + self_offsets[i+1] -
                                self_offsets[i] + 1)
            if dry_run:
                self.set_data_size(self.get_data_size() + next_data_offset
                                   - data_offset)
            else:
                self.data.put(self_offsets[i],
                              block.data.get(data_offset, next_data_offset),
                              dry_run)
            data_offset = next_data_offset
        message = (f'Block {self} of shape {self.shape} uses '
                   f'{self.data.mem_usage()}B of memory')
        assert(self.data.mem_usage() <= math.prod(self.shape)), message
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
        if self.data.mem_usage() == math.prod(self.shape):
            # don't read the block again if it was already read
            # TODO: investigate why this is happening
            return self.data.mem_usage()

        log(f'<< Reading {self.file_name}', 0)
        with open(self.file_name, 'rb') as f:
            self.data.put(0, f.read())
        message = (f'Block contains {self.data.mem_usage()}B but shape is '
                  f' {math.prod(self.shape)}B')
        assert(self.data.mem_usage() == math.prod(self.shape)), message
        return self.data.mem_usage()

    def read_from(self, block, dry_run=False):
        '''
        Read the relevant data sections of self from block's file name.
        In general, block doesn't have the same origin or shape as self.

        Return: (total_bytes, seeks), the total number of bytes read and the
        number of seeks required in block.

        Similar to write_to but for reading
        '''

        if not self.overlap(block):
            return 0, 0

        data = bytearray()
        origin, shape, block_offsets = block.block_offsets(self)
        if len(block_offsets) == 0:
            return 0, 0  # nothing to read
        # Read in block
        seeks = len(block_offsets)/2
        est_total_bytes = sum([block_offsets[i+1]-block_offsets[i] + 1
                              if i % 2 == 0 else 0
                              for i in range(len(block_offsets))])
        if dry_run:
            self.set_data_size(self.get_data_size() + est_total_bytes)
            return est_total_bytes, seeks
        with open(block.file_name, 'rb') as f:
            log(f'<< Reading from {block.file_name}'
                f' ({len(block_offsets)/2} seeks)', 0)
            total_bytes = 0
            for i, r in enumerate(block_offsets):
                if i % 2 == 1:
                    continue
                f.seek(block_offsets[i])
                data += f.read(block_offsets[i+1]-block_offsets[i] + 1)
                total_bytes += block_offsets[i+1]-block_offsets[i] + 1
            assert(len(data) == total_bytes), (f'Data size: {len(data)}, '
                                               'read {total_bytes} bytes '
                                               ' from block {block}')
            log(f'Read {total_bytes} bytes', 0)

        # Write data block to self
        data_block = Block(origin=origin, shape=shape, data=data)
        self.put_data_block(data_block)
        assert(total_bytes == est_total_bytes)
        return total_bytes, seeks

    def set_data_size(self, size):
        '''
        Set the data size of the data buffer. For use in dry mode executions.
        '''
        self.data.set_data_size(size)

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

    def write_to(self, block, dry_run=False):
        '''
        Write relevant data sections of self to block's file name

        Return: (total_bytes, seeks), the total number of bytes written and the
        number of seeks required in block.

        Similar to read_from but for writes.
        '''
        if not self.overlap(block):
            return 0, 0

        assert(block.file_name), f"Block {block} has no file name"

        data_b = self.get_data_block(block, dry_run)
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
            total_bytes = 0
            for i, r in enumerate(block_offsets):
                if i % 2 == 1:
                    continue
                f.seek(block_offsets[i])
                next_data_offset = (data_offset +
                                    block_offsets[i+1] -
                                    block_offsets[i] + 1)
                if dry_run:
                    wrote_bytes = next_data_offset - data_offset
                else:
                    wrote_bytes = f.write(data.get(data_offset,
                                                   next_data_offset))
                total_bytes += wrote_bytes
                data_offset = next_data_offset
            if total_bytes != 0:
                log(f'  Wrote {total_bytes} bytes to {block.file_name} ({len(block_offsets)/2} seeks)', 0)
        f.close()
        return total_bytes, seeks


def log(message, level=0):
    '''
    Temporary logger
    '''
    LOG_LEVEL = 1
    if level >= LOG_LEVEL:
        print(message)
