import os
from keep import keep
from keep.log import log


class Cache():
    '''
    An abstract Cache class for use in the repartition function

    Derived classes must implement the following methods:

    def insert(self, read_block, dry_run):
        raise Exception('Implement in sub-class')

    def mem_usage(self):
        raise Exception('Implement in sub-class')
    '''


class KeepCache(Cache):
    def __init__(self, out_blocks, match):
        '''
        out_blocks: a partition
        match: matching between read blocks and write blocks
        '''
        self.out_blocks = out_blocks
        self.match = match
        # self.dry_run = dry_run
        # log('Cache match:')
        # for k in match:
        #     log(k)
        #     log(match[k])

    def insert(self, read_block, dry_run):
        f_blocks = keep.get_F_blocks(read_block, self.out_blocks,
                                     get_data=True, dry_run=dry_run)
        complete_blocks = []
        for i in range(8):
            if f_blocks[i] is None or f_blocks[i].empty():
                continue
            dest_block = self.match[(read_block.origin, i)]
            dest_block.put_data_block(f_blocks[i], dry_run)  # in-memory copy
            if dest_block.complete():
                complete_blocks += [dest_block]
        # return the list of write blocks that are ready to be written
        return complete_blocks

    def mem_usage(self):
        blocks = {self.match[b] for b in self.match}
        return sum([b.mem_usage() for b in blocks])

    def __str__(self):
        blocks = ''
        for b in self.match:
            blocks += str(self.match[b]) + os.linesep
        return f'''
*** Cache ***

match_blocks: {blocks}

TOTAL data in mem: {self.mem_usage()}B
        '''


class BaselineCache(Cache):
    '''
    This looks like a cache but really isn't
    '''

    def __init__(self):
        self.block = None

    def insert(self, read_block, dry_run):
        self.block = read_block
        return [read_block]  # read block is just returned, to be written

    def mem_usage(self):
        return self.block.mem_usage()
