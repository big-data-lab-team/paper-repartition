import os
import pytest
from keep import keep
from keep.partition import Partition


def test_seek_model():
    array = Partition((2, 2, 2), name='array', fill='random')
    out_blocks = Partition((2, 1, 2), name='out', array=array)

    _, _, seeks, _ = keep.keep(array, out_blocks, None, array)
    assert(seeks == 3)

    _, _, seeks, _ = keep.baseline(out_blocks, array, None, array)
    assert(seeks == 6)


def test_get_f_blocks():
    array = Partition((12, 12, 12), name='array', fill='random')
    in_blocks = Partition((4, 4, 4), name='in', array=array)
    out_blocks = Partition((3, 3, 3), name='out', array=array)

    fblocks = keep.get_F_blocks(in_blocks.blocks[(0, 0, 0)], out_blocks)

    assert([str(b) for b in fblocks] == [('Block: origin (0, 0, 0); shape'
                                          ' (3, 3, 3); data in mem: 0B'),
                                         ('Block: origin (0, 0, 3); shape'
                                          ' (3, 3, 1); data in mem: 0B'),
                                         ('Block: origin (0, 3, 0); shape'
                                          ' (3, 1, 3); data in mem: 0B'),
                                         ('Block: origin (0, 3, 3); shape'
                                          ' (3, 1, 1); data in mem: 0B'),
                                         ('Block: origin (3, 0, 0); shape'
                                          ' (1, 3, 3); data in mem: 0B'),
                                         ('Block: origin (3, 0, 3); shape'
                                          ' (1, 3, 1); data in mem: 0B'),
                                         ('Block: origin (3, 3, 0); shape'
                                          ' (1, 1, 3); data in mem: 0B'),
                                         ('Block: origin (3, 3, 3); shape'
                                          ' (1, 1, 1); data in mem: 0B')])


def test_get_f_blocks_1():
    array = Partition((12, 12, 12), name='array', fill='random')
    in_blocks = Partition((4, 4, 4), name='in', array=array)

    fblocks = keep.get_F_blocks(array.blocks[(0, 0, 0)], in_blocks)

    assert([str(b) for b in fblocks] ==
           ['Block: origin (0, 0, 0); shape (12, 12, 12); data in mem: 0B',
            'None', 'None', 'None', 'None', 'None', 'None', 'None'])


def test_r_hat():
    array = Partition((3500, 3500, 3500), name='array')
    in_blocks = Partition((875, 875, 875), array=array, name='in')
    out_blocks = Partition((700, 875, 700), array=array, name='out')

    r_hat = keep.get_r_hat(in_blocks, out_blocks)
    assert(r_hat == (875, 875, 875))


def test_r_hat_1():
    array = Partition((20, 20, 20), name='array')
    in_blocks = Partition((20, 20, 20), array=array, name='in')
    out_blocks = Partition((20, 10, 2), array=array, name='out')

    r_hat = keep.get_r_hat(in_blocks, out_blocks)
    assert(r_hat == (20, 20, 20))


def test_r_hat_2():
    array = Partition((10, 10, 10), name='array')
    in_blocks = Partition((2, 2, 2), array=array, name='in')
    out_blocks = Partition((5, 5, 5), array=array, name='out')
    with pytest.raises(Exception):
        r_hat = keep.get_r_hat(in_blocks, out_blocks)


def test_divisors():
    assert(sorted(keep.divisors(10)) == [1, 2, 5, 10])
    assert(sorted(keep.divisors(42)) == [1, 2, 3, 6, 7, 14, 21, 42])


# def test_find_shape_with_constraint():
#     array = Partition((100, 100, 100), name='array', fill='random')
#     in_blocks = Partition((10, 10, 10), name='out', array=array)
#     out_blocks = Partition((50, 50, 50), name='out', array=array)
#     shape, mc = keep.find_shape_with_constraint(in_blocks, out_blocks, None)
#     assert((shape, mc) == ((50, 50, 50), 125000))

#     shape, mc = keep.find_shape_with_constraint(in_blocks, out_blocks, 3000)
#     assert((shape, mc) == ((1, 50, 50), 2500))


def test_partition_to_end_coords():
    d = 12
    array = Partition((d, d, d), name='array')
    in_blocks = Partition((4, 4, 4), name='in', array=array)
    coords = keep.partition_to_end_coords(in_blocks)
    assert(coords == ([3, 7, 11], [3, 7, 11], [3, 7, 11]))

    d = 3500
    array = Partition((d, d, d), name='array')
    in_blocks = Partition((500, 500, 500), name='in', array=array)
    coords = keep.partition_to_end_coords(in_blocks)
    assert(coords == ([499, 999, 1499, 1999, 2499, 2999, 3499],
                      [499, 999, 1499, 1999, 2499, 2999, 3499],
                      [499, 999, 1499, 1999, 2499, 2999, 3499]))

# def test_seeks(cleanup_blocks):
#     for a in (1, 2, 3, 4):
#         array = Partition((a, a, a), name='array')
#         divisors = keep.divisors(a)
#         configs = [(i, j, k)
#                    for i in divisors for j in divisors for k in divisors]
#         for c in configs:
#             for d in configs:
#                 in_blocks = Partition(c, name='in', array=array,
# fill='zeros')
#                 in_blocks.write()
#                 out_blocks = Partition(d, name='out', array=array)
#                 # raises an exception if seek count doesnt match real
#                 in_blocks.repartition(out_blocks, None, keep.baseline)
