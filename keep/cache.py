import keep

class Cache():

    def __init__(self, write_blocks, out_blocks, match):
        '''
        out_blocks: a partition
        write_blocks: 
        match: matching between read blocks and write blocks
        '''
        self.out_blocks = out_blocks
        self.write_blocks = write_blocks
        self.match = match
        print(match)

    def insert(self, read_block):
        print(f'Inserting read block {read_block.origin} in cache')
        f_blocks = keep.get_F_blocks(read_block, self.out_blocks)
        complete_blocks = []
        for i in range(8):
            if f_blocks[i] is None or f_blocks[i].empty():
                continue
            dest_block = self.match[(read_block.origin, i)]
            if dest_block.data  != f_blocks[i].data: # happens in baseline
                dest_block.put_data_block(f_blocks[i]) # in-memory copy, might be costly
            if dest_block.complete():
                complete_blocks += [ dest_block ]

        print(self)
        # return the list of write blocks that are ready to be written
        return complete_blocks

    def __str__(self):
        return f'''
*** Cache ***

write_blocks: {self.write_blocks}
        '''