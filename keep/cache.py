class Cache():

    def __init__(self, write_blocks, out_blocks):
        '''
        out_blocks: a partition
        write_blocks: 
        '''
        self.out_blocks = out_blocks
        self.write_blocks = write_blocks

    def insert(self, read_block):
        print(read_block.origin)
        self.write_blocks.blocks[read_block.origin][0].data = read_block.data # assume read and write blocks are identical
        return [ self.write_blocks.blocks[read_block.origin][0] ] # return the list of write blocks that are ready to be written