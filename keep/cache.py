import keep


class Cache():
    '''
    An abstract Cache class for use in the repartition function
    '''
    def insert(self, read_block):
        raise Exception('Implement in sub-class')

    def mem_usage(self):
        raise Exception('Implement in sub-class')


class KeepCache(Cache):
    def __init__(self, out_blocks, match):
        '''
        out_blocks: a partition
        match: matching between read blocks and write blocks
        '''
        self.out_blocks = out_blocks
        self.match = match
        log('Cache match:')
        for k in match:
            log(k)
            log(match[k])

    def insert(self, read_block):
        f_blocks = keep.get_F_blocks(read_block, self.out_blocks,
                                     get_data=True)
        complete_blocks = []
        for i in range(8):
            if f_blocks[i] is None or f_blocks[i].empty():
                continue
            dest_block = self.match[(read_block.origin, i)]
            dest_block.put_data_block(f_blocks[i])  # in-memory copy
            if dest_block.complete():
                complete_blocks += [dest_block]

        log(self)
        # return the list of write blocks that are ready to be written
        return complete_blocks

    def mem_usage(self):
        return sum([self.match[b].mem_usage() for b in self.match])

    def __str__(self):
        return f'''
*** Cache ***

match_blocks: {self.match_blocks}

TOTAL data in mem: {self.mem_usage()}B
        '''


class BaselineCache(Cache):
    '''
    This looks like a cache but really isn't
    '''

    def __init__(self):
        self.block = None

    def insert(self, read_block):
        self.block = read_block
        return [read_block]  # read block is just returned, to be written

    def mem_usage(self):
        if self.block is None:
            return 0
        return self.block.mem_usage()


def log(message, level=0):
    LOG_LEVEL = 1
    if level >= LOG_LEVEL:
        print(message)