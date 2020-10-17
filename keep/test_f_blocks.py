import keep
from partition import Partition

array = Partition((12, 12, 12), name='array', fill='random')
in_blocks = Partition((4, 4, 4), name='in', array=array)

fblocks = keep.get_F_blocks(array.blocks[(0,0,0)], in_blocks)

assert([str(b) for b in fblocks] == ['Block: origin (0, 0, 0); shape (12, 12, 12); data in mem: 0B',
                                     'None', 'None', 'None', 'None', 'None', 'None', 'None'])

array = Partition((12, 12, 12), name='array', fill='random')
in_blocks = Partition((4, 4, 4), name='in', array=array)
out_blocks = Partition((3, 3, 3), name='out', array=array)

fblocks = keep.get_F_blocks(in_blocks.blocks[(0,0,0)], out_blocks)

assert([str(b) for b in fblocks] == ['Block: origin (0, 0, 0); shape (3, 3, 3); data in mem: 0B',
                                     'Block: origin (0, 0, 3); shape (3, 3, 1); data in mem: 0B',
                                     'Block: origin (0, 3, 0); shape (3, 1, 3); data in mem: 0B',
                                     'Block: origin (0, 3, 3); shape (3, 1, 1); data in mem: 0B',
                                     'Block: origin (3, 0, 0); shape (1, 3, 3); data in mem: 0B',
                                     'Block: origin (3, 0, 3); shape (1, 3, 1); data in mem: 0B',
                                     'Block: origin (3, 3, 0); shape (1, 1, 3); data in mem: 0B',
                                     'Block: origin (3, 3, 3); shape (1, 1, 1); data in mem: 0B'])