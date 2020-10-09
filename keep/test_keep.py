import glob
import keep
import math
import os
import pytest
import keep
from partition import Partition
from block import Block

@pytest.fixture
def cleanup_blocks():
    yield
    for f in glob.glob('*.bin'):
        os.remove(f)

def test_inside():
    b = Block((4, 5, 6), (2, 4, 6))
    assert(b.inside((5, 8, 11)))
    assert(not b.inside((3, 5, 6)))
    assert(not b.inside((5, 10, 7)))


def test_offset():
    b = Block((1, 1, 1), (4, 5, 6))
    assert(b.offset((1, 1, 3)) == 2)
    assert(b.offset((2, 2, 2)) == 1+6+30)

    c = Block((1,2,3), (5, 6, 7))
    assert(c.offset((1, 2, 9))==6)
    assert(c.offset((1, 7, 3))==35)
    assert(c.offset((1, 7, 9))==41)
    assert(c.offset((2, 2, 3))==42)
    assert(c.offset((5, 7, 9))==math.prod(c.shape)-1)


def test_block_offsets():
    b = Block((1, 2, 3), (5, 6, 7))
    assert((b.block_offsets(b)) == (b.origin, b.shape, (((1, 2, 3), 0), ((5, 7, 9), 209))))
    c = Block((0, 0, 0), (4, 4, 4))
    assert(c.block_offsets(b) == (b.origin,
                                  (3, 2, 1),
                                  (((1, 2, 3), 27),
                                   ((1, 2, 3), 27),
                                   ((1, 3, 3), 31),
                                   ((1, 3, 3), 31),
                                   ((2, 2, 3), 43),
                                   ((2, 2, 3), 43),
                                   ((2, 3, 3), 47),
                                   ((2, 3, 3), 47),
                                   ((3, 2, 3), 59),
                                   ((3, 2, 3), 59),
                                   ((3, 3, 3), 63),
                                   ((3, 3, 3), 63))))
    d = Block((1, 2, 2), (4, 4, 4))
    assert(c.block_offsets(d) == (d.origin,
                                  (3, 2, 2),
                                  (((1, 2, 2), 26),
                                   ((1, 2, 3), 27),
                                   ((1, 3, 2), 30),
                                   ((1, 3, 3), 31),
                                   ((2, 2, 2), 42),
                                   ((2, 2, 3), 43),
                                   ((2, 3, 2), 46),
                                   ((2, 3, 3), 47),
                                   ((3, 2, 2), 58),
                                   ((3, 2, 3), 59),
                                   ((3, 3, 2), 62),
                                   ((3, 3, 3), 63))))
    e = Block((1, 2, 1), (4, 4, 4))
    assert(c.block_offsets(e) == (e.origin,
                                  (3, 2, 3),
                                  (((1, 2, 1), 25),
                                   ((1, 2, 3), 27),
                                   ((1, 3, 1), 29),
                                   ((1, 3, 3), 31),
                                   ((2, 2, 1), 41),
                                   ((2, 2, 3), 43),
                                   ((2, 3, 1), 45),
                                   ((2, 3, 3), 47),
                                   ((3, 2, 1), 57),
                                   ((3, 2, 3), 59),
                                   ((3, 3, 1), 61),
                                   ((3, 3, 3), 63))))

def test_write_to_shape_match(cleanup_blocks):

    b = Block((1, 2, 3), (5, 6, 7), fill='random', file_name='test.bin')
    l, _ = b.write_to(b)
    assert(l == math.prod(b.shape))

    with open(b.file_name, 'rb') as f:
        data = f.read()
    assert(data == b.data)
    os.remove(b.file_name)

def test_write_to_shape_mismatch(cleanup_blocks):
    b = Block((1, 2, 3), (5, 6, 7), fill='random', file_name='test.bin')
    c = Block((1, 2, 3), (5, 2, 7), file_name='block1.bin')
    d = Block((1, 4, 3), (5, 4, 7), file_name='block2.bin')
    l, _ = b.write_to(c)
    m, _ = b.write_to(d)
    assert(l + m == math.prod(b.shape))

    # Check content
    c.read()
    assert(c.data[:10] == b.data[:10])
    d.read()
    assert(d.data[-10:] == b.data[-10:])
    for fn in (c.file_name, d.file_name):
        os.remove(fn)

def test_read_from_shape_match(cleanup_blocks):
    b = Block((1, 2, 3), (5, 6, 7), fill='random', file_name='test.bin')
    b.write()

    l, _ = b.read_from(b)
    assert(l == math.prod(b.shape))

    with open(b.file_name, 'rb') as f:
        data = f.read()
    assert(data == b.data)
    os.remove(b.file_name)

def test_read_from_shape_mismatch(cleanup_blocks):
    b = Block((1, 2, 3), (5, 6, 7), fill='random', file_name='test.bin')
    c = Block((1, 2, 3), (5, 2, 7), fill='random', file_name='block1.bin')
    d = Block((1, 4, 3), (5, 4, 7), fill='random', file_name='block2.bin')
    c.write()
    d.write()
    l, _ = b.read_from(c)
    m, _ = b.read_from(d)
    assert(l + m == math.prod(b.shape))

    # Check content
    assert(c.data[:10] == b.data[:10])
    assert(d.data[-10:] == b.data[-10:])
    for fn in (c.file_name, d.file_name):
        os.remove(fn)

def test_integration_write_to_read_from(cleanup_blocks):
    b = Block((1, 2, 3), (5, 6, 7), fill='random', file_name='test.bin')
    c = Block((1, 2, 3), (5, 2, 7), file_name='block1.bin')
    d = Block((1, 4, 3), (5, 4, 7), file_name='block2.bin')
    l, _ = b.write_to(c)
    m, _ = b.write_to(d)

    original_data = bytearray()
    original_data[:] = b.data
    b.read_from(c)
    b.read_from(d)
    assert(b.data == original_data)
    for fn in (c.file_name, d.file_name):
        os.remove(fn)

def test_repartition_baseline(cleanup_blocks):
    array = Partition((2, 2, 2), name='array', fill='random')
    array.write()
    out_blocks = Partition((2, 1, 2), name='out', array=array)
    array.repartition(out_blocks, -1, keep.baseline)
    array.blocks[(0,0,0)].read()
    out_blocks.blocks[(0,0,0)].read()
    out_blocks.blocks[(0,1,0)].read()

    assert(array.blocks[(0,0,0)].data[:2] == out_blocks.blocks[(0,0,0)].data[:2])
    assert(array.blocks[(0,0,0)].data[2:4] == out_blocks.blocks[(0,1,0)].data[:2])
    assert(array.blocks[(0,0,0)].data[4:6] == out_blocks.blocks[(0,0,0)].data[2:4])
    assert(array.blocks[(0,0,0)].data[6:8] == out_blocks.blocks[(0,1,0)].data[2:4])

    rein_blocks = Partition((2, 2, 2), name='rein')
    out_blocks.repartition(rein_blocks, -1, keep.baseline)
    rein_blocks.blocks[(0,0,0)].read()

    print(array.blocks[(0,0,0)].data)
    print(rein_blocks.blocks[(0,0,0)].data)
    assert(array.blocks[(0,0,0)].data == rein_blocks.blocks[(0,0,0)].data)

def test_repartition_baseline_1(cleanup_blocks):
    array = Partition((5, 6, 7), name='array', fill='random')
    array.write()
    out_blocks = Partition((5, 3, 7), name='out', array=array)
    array.repartition(out_blocks, -1, keep.baseline)
    array.blocks[(0,0,0)].read()
    out_blocks.blocks[(0,0,0)].read()
    out_blocks.blocks[(0,3,0)].read()

    assert(array.blocks[(0,0,0)].data[:20] == out_blocks.blocks[(0,0,0)].data[:20])
    assert(array.blocks[(0,0,0)].data[-20:] == out_blocks.blocks[(0,3,0)].data[-20:])


    rein_blocks = Partition((5, 6, 7), name='rein')
    out_blocks.repartition(rein_blocks, -1, keep.baseline)
    rein_blocks.blocks[(0,0,0)].read()

    assert(array.blocks[(0,0,0)].data == rein_blocks.blocks[(0,0,0)].data)

def test_repartition_baseline_2(cleanup_blocks):
    array = Partition((10, 20, 30), name='array')
    in_blocks = Partition((10, 20, 30), name='in', array=array, fill='random')
    in_data = in_blocks.blocks[(0,0,0)].data
    in_blocks.write()
    out_blocks = Partition((10, 10, 15), name='out', array=array)
    in_blocks.repartition(out_blocks, -1, keep.baseline)

    rein_blocks = Partition((10, 20, 30), name='rein', array=array)
    out_blocks.repartition(rein_blocks, -1, keep.baseline)
    rein_blocks.blocks[(0,0,0)].read()
    rein_data = rein_blocks.blocks[(0,0,0)].data

    assert(rein_data == in_data)

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

def test_divisors():
    assert(sorted(keep.divisors(10)) == [1, 2, 5, 10])
    assert(sorted(keep.divisors(42)) == [1, 2, 3, 6, 7, 14, 21, 42])

def test_candidate_read_shapes():
    array = Partition((3500, 3500, 3500), name='array')
    in_blocks = Partition((875, 875, 875), array=array, name='in')
    out_blocks = Partition((700, 875, 700), array=array, name='out')
    r_hat = keep.get_r_hat(in_blocks, out_blocks)
    read_shapes = keep.candidate_read_shapes(in_blocks, out_blocks, r_hat, array)
    # TODO: add assertion on read shapes