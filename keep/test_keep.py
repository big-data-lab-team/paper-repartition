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

def test_zeros():
    b = Block((4, 5, 6), (2, 4, 6), fill='zeros')
    assert(b.data.bytes() == bytearray(math.prod(b.shape)))

def test_read_from_disjoint():
    b = Block((4, 5, 6), (2, 4, 6))
    c = Block((40, 50, 60), (2, 4, 6))
    assert(b.read_from(c) == (0, 0))


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
    assert(data == b.data.bytes())
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
    assert(c.data.bytes()[:10] == b.data.bytes()[:10])
    d.read()
    assert(d.data.bytes()[-10:] == b.data.bytes()[-10:])
    for fn in (c.file_name, d.file_name):
        os.remove(fn)

def test_read_from_shape_match(cleanup_blocks):
    b = Block((1, 2, 3), (5, 6, 7), fill='random', file_name='test.bin')
    b.write()

    l, _ = b.read_from(b)
    assert(l == math.prod(b.shape))

    with open(b.file_name, 'rb') as f:
        data = f.read()
    assert(data == b.data.bytes())
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
    assert(c.data.bytes()[:10] == b.data.bytes()[:10])
    assert(d.data.bytes()[-10:] == b.data.bytes()[-10:])
    for fn in (c.file_name, d.file_name):
        os.remove(fn)

def test_integration_write_to_read_from(cleanup_blocks):
    b = Block((1, 2, 3), (5, 6, 7), fill='random', file_name='test.bin')
    c = Block((1, 2, 3), (5, 2, 7), file_name='block1.bin')
    d = Block((1, 4, 3), (5, 4, 7), file_name='block2.bin')
    l, _ = b.write_to(c)
    m, _ = b.write_to(d)

    original_data = bytearray()
    original_data[:] = b.data.bytes()
    b.read_from(c)
    b.read_from(d)
    assert(b.data.bytes() == original_data)
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

    assert(array.blocks[(0,0,0)].data.bytes()[:2] == out_blocks.blocks[(0,0,0)].data.bytes()[:2])
    assert(array.blocks[(0,0,0)].data.bytes()[2:4] == out_blocks.blocks[(0,1,0)].data.bytes()[:2])
    assert(array.blocks[(0,0,0)].data.bytes()[4:6] == out_blocks.blocks[(0,0,0)].data.bytes()[2:4])
    assert(array.blocks[(0,0,0)].data.bytes()[6:8] == out_blocks.blocks[(0,1,0)].data.bytes()[2:4])

    rein_blocks = Partition((2, 2, 2), name='rein')
    out_blocks.repartition(rein_blocks, -1, keep.baseline)
    rein_blocks.blocks[(0,0,0)].read()

    assert(array.blocks[(0,0,0)].data.bytes() == rein_blocks.blocks[(0,0,0)].data.bytes())

def test_repartition_baseline_1(cleanup_blocks):
    array = Partition((5, 6, 7), name='array', fill='random')
    array.write()
    out_blocks = Partition((5, 3, 7), name='out', array=array)
    array.repartition(out_blocks, -1, keep.baseline)
    array.blocks[(0,0,0)].read()
    out_blocks.blocks[(0,0,0)].read()
    out_blocks.blocks[(0,3,0)].read()

    assert(array.blocks[(0,0,0)].data.bytes()[:20] == out_blocks.blocks[(0,0,0)].data.bytes()[:20])
    assert(array.blocks[(0,0,0)].data.bytes()[-20:] == out_blocks.blocks[(0,3,0)].data.bytes()[-20:])


    rein_blocks = Partition((5, 6, 7), name='rein')
    out_blocks.repartition(rein_blocks, -1, keep.baseline)
    rein_blocks.blocks[(0,0,0)].read()

    assert(array.blocks[(0,0,0)].data.bytes() == rein_blocks.blocks[(0,0,0)].data.bytes())

def test_repartition_baseline_2(cleanup_blocks):
    array = Partition((10, 20, 30), name='array')
    in_blocks = Partition((10, 20, 30), name='in', array=array, fill='random')
    in_data = in_blocks.blocks[(0,0,0)].data.bytes()
    in_blocks.write()
    out_blocks = Partition((10, 10, 15), name='out', array=array)
    in_blocks.repartition(out_blocks, -1, keep.baseline)

    rein_blocks = Partition((10, 20, 30), name='rein', array=array)
    out_blocks.repartition(rein_blocks, -1, keep.baseline)
    rein_blocks.blocks[(0,0,0)].read()
    rein_data = rein_blocks.blocks[(0,0,0)].data.bytes()

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

def test_repartition_keep(cleanup_blocks):
    array = Partition((2, 2, 2), name='array', fill='random')
    array.write()
    out_blocks = Partition((2, 1, 2), name='out', array=array)
    array.repartition(out_blocks, -1, keep.keep)
    array.blocks[(0,0,0)].read()
    out_blocks.blocks[(0,0,0)].read()
    out_blocks.blocks[(0,1,0)].read()

    assert(array.blocks[(0,0,0)].data.bytes()[:2] == out_blocks.blocks[(0,0,0)].data.bytes()[:2])
    assert(array.blocks[(0,0,0)].data.bytes()[2:4] == out_blocks.blocks[(0,1,0)].data.bytes()[:2])
    assert(array.blocks[(0,0,0)].data.bytes()[4:6] == out_blocks.blocks[(0,0,0)].data.bytes()[2:4])
    assert(array.blocks[(0,0,0)].data.bytes()[6:8] == out_blocks.blocks[(0,1,0)].data.bytes()[2:4])

    rein_blocks = Partition((2, 2, 2), name='rein')
    out_blocks.repartition(rein_blocks, -1, keep.keep)
    rein_blocks.blocks[(0,0,0)].read()

def test_repartition_keep_1(cleanup_blocks):
    array = Partition((5, 6, 7), name='array', fill='random')
    array.write()
    out_blocks = Partition((5, 3, 7), name='out', array=array)
    array.repartition(out_blocks, -1, keep.keep)
    array.blocks[(0,0,0)].read()
    out_blocks.blocks[(0,0,0)].read()
    out_blocks.blocks[(0,3,0)].read()

    assert(array.blocks[(0,0,0)].data.bytes()[:20] == out_blocks.blocks[(0,0,0)].data.bytes()[:20])
    assert(array.blocks[(0,0,0)].data.bytes()[-20:] == out_blocks.blocks[(0,3,0)].data.bytes()[-20:])


    rein_blocks = Partition((5, 6, 7), name='rein')
    out_blocks.repartition(rein_blocks, -1, keep.keep)
    rein_blocks.blocks[(0,0,0)].read()

    assert(array.blocks[(0,0,0)].data.bytes() == rein_blocks.blocks[(0,0,0)].data.bytes())

def test_repartition_keep_2(cleanup_blocks):
    array = Partition((10, 20, 30), name='array')
    in_blocks = Partition((10, 20, 30), name='in', array=array, fill='random')
    in_data = in_blocks.blocks[(0,0,0)].data.bytes()
    in_blocks.write()
    out_blocks = Partition((10, 10, 15), name='out', array=array)
    in_blocks.repartition(out_blocks, -1, keep.keep)

    rein_blocks = Partition((10, 20, 30), name='rein', array=array)
    out_blocks.repartition(rein_blocks, -1, keep.keep)
    rein_blocks.blocks[(0,0,0)].read()
    rein_data = rein_blocks.blocks[(0,0,0)].data.bytes()

    assert(rein_data == in_data)

def test_repartition_baseline_3(cleanup_blocks):
    array = Partition((12, 12, 12), name='array', fill='random')
    array.write()
    in_blocks = Partition((4, 4, 4), name='in', array=array)
    array.repartition(in_blocks, -1, keep.baseline)


    out_blocks = Partition((3, 3, 3), name='out', array=array)
    in_blocks.repartition(out_blocks, -1, keep.baseline)

    rein_blocks = Partition((12, 12, 12), name='rein', array=array)
    out_blocks.repartition(rein_blocks, -1, keep.baseline)

    rein_blocks.blocks[(0,0,0)].read()
    rein_data = rein_blocks.blocks[(0,0,0)].data.bytes()

    array.blocks[(0,0,0)].read()
    array_data = array.blocks[(0,0,0)].data.bytes()

    assert(rein_data == array_data)

def test_repartition_keep_3(cleanup_blocks):
    array = Partition((12, 12, 12), name='array', fill='random')
    array.write()
    in_blocks = Partition((4, 4, 4), name='in', array=array)
    array.repartition(in_blocks, -1, keep.keep)


    out_blocks = Partition((3, 3, 3), name='out', array=array)
    in_blocks.repartition(out_blocks, -1, keep.keep)

    rein_blocks = Partition((12, 12, 12), name='rein', array=array)
    out_blocks.repartition(rein_blocks, -1, keep.keep)

    rein_blocks.blocks[(0,0,0)].read()
    rein_data = rein_blocks.blocks[(0,0,0)].data.bytes()

    array.blocks[(0,0,0)].read()
    array_data = array.blocks[(0,0,0)].data.bytes()

    assert(rein_data == array_data)

def test_partition_clear(cleanup_blocks):
    array = Partition((12, 12, 12), name='array', fill='random')
    array.clear()

def test_partition_to_end_coords():
    d = 12
    array = Partition((d, d, d), name='array')
    in_blocks = Partition((4, 4, 4), name='in', array=array)
    coords = keep.partition_to_end_coords(in_blocks)
    assert(coords == ([4, 8, 12], [4, 8, 12], [4, 8, 12]))

    d = 3500
    array = Partition((d, d, d), name='array')
    in_blocks = Partition((500, 500, 500), name='in', array=array)
    coords = keep.partition_to_end_coords(in_blocks)
    assert(coords == ([500, 1000, 1500, 2000, 2500, 3000, 3500],
                      [500, 1000, 1500, 2000, 2500, 3000, 3500],
                      [500, 1000, 1500, 2000, 2500, 3000, 3500]))

def test_seeks(cleanup_blocks):
    for a in (1, 2, 3, 4):
        array = Partition((a, a, a), name='array')
        divisors = keep.divisors(a)
        configs = [(i, j, k)
                   for i in divisors for j in divisors for k in divisors]
        for c in configs:
            for d in configs:
                in_blocks = Partition(c, name='in', array=array, fill='zeros')
                in_blocks.write()
                out_blocks = Partition(d, name='out', array=array)
                # raises an exception if seek count doesnt match real
                in_blocks.repartition(out_blocks, -1, keep.baseline)