import math
import os
import time
from keep.log import log


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
        self.data = [(0, data)]  # (offset, bytearray)
        self.mem_size = len(data)

    def get(self, start_offset=0, end_offset=None):
        '''
        Returns the data between start_offset (included)
        and end_offset (excluded)
        '''
        if len(self.data) > 1:
            self.merge_dict()
        return self.data[0][1][start_offset:end_offset]

    def clear(self):
        '''
        Clear the buffer content
        '''
        self.data = []
        self.mem_size = 0

    # def get(self, start_offset, end_offset):
    #     '''
    #     Returns the data between start_offset (included)
    #     and end_offset (excluded)
    #     '''
    #     return self.bytes(start_offset, end_offset)

    def mem_usage(self):
        '''
        Return the size of the data buffer in memory
        '''
        # Warning: this is not real memory usage due to Python overheads
        # and most importantly zero padding in put()
        return self.mem_size

    def merge_dict(self):
        '''
        Merge the data dictionary
        '''
        self.data.sort()
        self.data = [(0, b''.join([self.data[i][1]
                                   for i in range(len(self.data))]))]

    def put(self, offset, buffer, length):
        '''
        Insert buffer of length length at given offset in self. This
        function was the main
        motivation to create the class
        '''
        self.data += [(offset, buffer)]
        self.mem_size += length

    def put_all(self, datatuples, size):
        self.data += datatuples
        self.mem_size += size


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
        if fill is not None:
            self.write()
            self.clear()

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
        delta_1_block = block.shape[2] - (end[2] - origin[2])
        delta_0 = (self.shape[1] - (end[1] - origin[1]))*self.shape[2]
        delta_0_block = (block.shape[1] - (end[1] - origin[1]))*block.shape[2]

        current_offset = self.offset(origin)
        current_offset_block = block.offset(origin)
        read_points = []  # start new segment
        read_points_block = []
        n = 0
        start_seg = current_offset
        start_seg_block = current_offset_block
        for i in range(end[0]-origin[0]):
            for j in range(end[1]-origin[1]):
                if delta_2 != 0:
                    current_offset += delta_2  # extend segment
                    current_offset_block += delta_2
                if delta_1 != 0:
                    end_seg = current_offset - 1
                    end_seg_block = current_offset_block - 1
                    read_points += [start_seg, end_seg]
                    read_points_block += [start_seg_block, end_seg_block]
                    n += 2
                    current_offset += delta_1
                    current_offset_block += delta_1_block
                    start_seg = current_offset
                    start_seg_block = current_offset_block
            if delta_0 != 0:
                if delta_1 == 0:
                    end_seg = current_offset - 1
                    end_seg_block = current_offset_block - 1
                    read_points += [start_seg, end_seg]
                    read_points_block += [start_seg_block, end_seg_block]
                    n += 2
                current_offset += delta_0
                current_offset_block += delta_0_block
                start_seg = current_offset
                start_seg_block = current_offset_block

        end_seg = current_offset - 1
        end_seg_block = current_offset_block - 1
        if read_points == []:
            read_points += [start_seg, end_seg]
            read_points_block += [start_seg_block, end_seg_block]
            n += 2
        return (origin, tuple(end[i]-origin[i] for i in (0, 1, 2)),
                tuple(read_points), tuple(read_points_block), n)

    def clear(self):
        '''
        Clear the data buffer in the block
        '''
        self.data.clear()

    def delete(self):
        '''
        Delete the block from disk
        '''
        if os.path.isfile(self.file_name):
            os.remove(self.file_name)

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

        if not self.overlap(block):
            return Block((-1, -1, -1), (0, 0, 0))

        origin, shape, self_offsets, _, lb = self.block_offsets(block)

        data = b''.join([self.data.get(self_offsets[i], (self_offsets[i+1]+1))
                         for i in range(0, lb, 2)])
        return Block(origin, shape, data)

    def mem_usage(self):
        '''
        Return the memory usage of the block
        '''
        return self.data.mem_usage()

    def offset(self, point):
        '''
        Return offset of point in self
        '''
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

    def point_from_offset(self, offset):
        '''
        Return point coordinates from offset
        '''
        a = self.shape[2]*self.shape[1]
        x = self.origin[0] + (offset // a)
        b = offset % a
        y = self.origin[1] + (b // self.shape[2])
        z = self.origin[2] + (b % self.shape[2])
        return (x, y, z)

    def put_data_block(self, block):
        '''
        Write the relevant sections of block.data into self.data

        Similar to get_data_block but to copy from block to self
        '''
        # assert(self.data.mem_usage() <= math.prod(self.shape)), message
        if not self.overlap(block):
            return

        _, _, self_offsets, _, lb = self.block_offsets(block)

        data_offset = 0

        for i in range(0, lb, 2):
            next_data_offset = (data_offset + self_offsets[i+1] -
                                self_offsets[i] + 1)
            self.data.put(self_offsets[i],
                          block.data.get(data_offset, next_data_offset),
                          next_data_offset - data_offset)
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
        start = time.time()
        with open(self.file_name, 'rb') as f:
            data = f.read()
        read_time = time.time() - start
        self.data.put(0, data, len(data))
        message = (f'Block contains {self.data.mem_usage()}B but shape is '
                   f' {math.prod(self.shape)}B')
        assert(self.data.mem_usage() == math.prod(self.shape)), message
        return self.data.mem_usage(), read_time

    def read_from(self, block):
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
        origin, shape, _, _, lb = block.block_offsets(self)
        nbytes = math.prod(shape)
        if lb == 0:
            return 0, 0  # nothing to read

        data_block = Block(origin=origin, shape=shape)
        _, _, self_offsets, block_offsets, lc = self.block_offsets(data_block)

        # Read in block
        seeks = lb/2

        def foo(i):
            f.seek(block_offsets[i])
            return (self_offsets[i],
                    f.read(block_offsets[i+1] - block_offsets[i]+1))

        log(f'<< Reading from {block.file_name}'
            f' ({seeks} seeks)', 1)
        with open(block.file_name, 'rb') as f:
            datatuples = [(self_offsets[i],
                           f.read(block_offsets[i+1] - block_offsets[i]+1))
                          for i in range(0, lc, 2)
                          if f.seek(block_offsets[i]) >= 0]
        self.data.put_all(datatuples, nbytes)

        return nbytes, seeks, -1

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
        start = time.time()
        with open(self.file_name, 'wb+') as f:
            b = f.write(self.data.get(0, math.prod(self.shape)))
        return b, time.time() - start

    def write_to(self, block):
        '''
        Write relevant data sections of self to block's file name

        Return: (total_bytes, seeks), the total number of bytes written and the
        number of seeks required in block.

        Similar to read_from but for writes.
        '''
        if not self.overlap(block):
            return 0, 0

        assert(block.file_name), f"Block {block} has no file name"

        data_b = self.get_data_block(block)
        data = data_b.data

        _, _, block_offsets, _, lb = block.block_offsets(data_b)
        # block offsets are now the offsets in the block to be written

        data_offset = 0
        seeks = lb / 2
        mode = 'wb'
        if os.path.exists(block.file_name):
            # if file already exists, open in r+b mode
            #  to modify without overwriting
            mode = 'r+b'
        write_time = 0

        log(f'>> Writing to {block.file_name} ({seeks} seeks)', 1)
        with open(block.file_name, mode) as f:
            total_bytes = 0
            for i in range(0, lb, 2):
                next_data_offset = (data_offset +
                                    block_offsets[i+1] -
                                    block_offsets[i] + 1)
                start = time.time()
                f.seek(block_offsets[i])
                wrote_bytes = f.write(data.get(data_offset,
                                               next_data_offset))
                write_time += time.time() - start
                total_bytes += wrote_bytes
                data_offset = next_data_offset
            if total_bytes != 0:
                log(f'  Wrote {total_bytes} bytes to {block.file_name} '
                    f'({seeks} seeks)', 0)
        f.close()
        return total_bytes, seeks, write_time
