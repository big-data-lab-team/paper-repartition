import glob
import keep
import os
import pytest
from partition import Partition


@pytest.fixture
def cleanup_blocks():
    yield
    for f in glob.glob('*.bin'):
        os.remove(f)


def test_repartition_baseline(cleanup_blocks):
    array = Partition((2, 2, 2), name='array', fill='random')
    array.write()
    out_blocks = Partition((2, 1, 2), name='out', array=array)
    array.repartition(out_blocks, None, keep.baseline)
    array.blocks[(0, 0, 0)].read()
    out_blocks.blocks[(0, 0, 0)].read()
    out_blocks.blocks[(0, 1, 0)].read()

    assert(array.blocks[(0, 0, 0)].data.bytes()[:2] ==
           out_blocks.blocks[(0, 0, 0)].data.bytes()[:2])
    assert(array.blocks[(0, 0, 0)].data.bytes()[2:4] ==
           out_blocks.blocks[(0, 1, 0)].data.bytes()[:2])
    assert(array.blocks[(0, 0, 0)].data.bytes()[4:6] ==
           out_blocks.blocks[(0, 0, 0)].data.bytes()[2:4])
    assert(array.blocks[(0, 0, 0)].data.bytes()[6:8] ==
           out_blocks.blocks[(0, 1, 0)].data.bytes()[2:4])

    rein_blocks = Partition((2, 2, 2), name='rein')
    out_blocks.repartition(rein_blocks, None, keep.baseline)
    rein_blocks.blocks[(0, 0, 0)].read()

    assert(array.blocks[(0, 0, 0)].data.bytes() ==
           rein_blocks.blocks[(0, 0, 0)].data.bytes())


def test_repartition_baseline_1(cleanup_blocks):
    array = Partition((5, 6, 7), name='array', fill='random')
    array.write()
    out_blocks = Partition((5, 3, 7), name='out', array=array)
    array.repartition(out_blocks, None, keep.baseline)
    array.blocks[(0, 0, 0)].read()
    out_blocks.blocks[(0, 0, 0)].read()
    out_blocks.blocks[(0, 3, 0)].read()

    assert(array.blocks[(0, 0, 0)].data.bytes()[:20] ==
           out_blocks.blocks[(0, 0, 0)].data.bytes()[:20])
    assert(array.blocks[(0, 0, 0)].data.bytes()[-20:] ==
           out_blocks.blocks[(0, 3, 0)].data.bytes()[-20:])

    rein_blocks = Partition((5, 6, 7), name='rein')
    out_blocks.repartition(rein_blocks, None, keep.baseline)
    rein_blocks.blocks[(0, 0, 0)].read()

    assert(array.blocks[(0, 0, 0)].data.bytes() ==
           rein_blocks.blocks[(0, 0, 0)].data.bytes())


def test_repartition_baseline_2(cleanup_blocks):
    array = Partition((10, 20, 30), name='array')
    in_blocks = Partition((10, 20, 30), name='in', array=array, fill='random')
    in_data = in_blocks.blocks[(0, 0, 0)].data.bytes()
    in_blocks.write()
    out_blocks = Partition((10, 10, 15), name='out', array=array)
    in_blocks.repartition(out_blocks, None, keep.baseline)

    rein_blocks = Partition((10, 20, 30), name='rein', array=array)
    out_blocks.repartition(rein_blocks, None, keep.baseline)
    rein_blocks.blocks[(0, 0, 0)].read()
    rein_data = rein_blocks.blocks[(0, 0, 0)].data.bytes()

    assert(rein_data == in_data)


def test_repartition_keep(cleanup_blocks):
    array = Partition((2, 2, 2), name='array', fill='random')
    array.write()
    out_blocks = Partition((2, 1, 2), name='out', array=array)
    array.repartition(out_blocks, None, keep.keep)
    array.blocks[(0, 0, 0)].read()
    out_blocks.blocks[(0, 0, 0)].read()
    out_blocks.blocks[(0, 1, 0)].read()

    assert(array.blocks[(0, 0, 0)].data.bytes()[:2] ==
           out_blocks.blocks[(0, 0, 0)].data.bytes()[:2])
    assert(array.blocks[(0, 0, 0)].data.bytes()[2:4] ==
           out_blocks.blocks[(0, 1, 0)].data.bytes()[:2])
    assert(array.blocks[(0, 0, 0)].data.bytes()[4:6] ==
           out_blocks.blocks[(0, 0, 0)].data.bytes()[2:4])
    assert(array.blocks[(0, 0, 0)].data.bytes()[6:8] ==
           out_blocks.blocks[(0, 1, 0)].data.bytes()[2:4])

    rein_blocks = Partition((2, 2, 2), name='rein')
    out_blocks.repartition(rein_blocks, None, keep.keep)
    rein_blocks.blocks[(0, 0, 0)].read()


def test_repartition_keep_1(cleanup_blocks):
    array = Partition((5, 6, 7), name='array', fill='random')
    array.write()
    out_blocks = Partition((5, 3, 7), name='out', array=array)
    array.repartition(out_blocks, None, keep.keep)
    array.blocks[(0, 0, 0)].read()
    out_blocks.blocks[(0, 0, 0)].read()
    out_blocks.blocks[(0, 3, 0)].read()

    assert(array.blocks[(0, 0, 0)].data.bytes()[:20] ==
           out_blocks.blocks[(0, 0, 0)].data.bytes()[:20])
    assert(array.blocks[(0, 0, 0)].data.bytes()[-20:] ==
           out_blocks.blocks[(0, 3, 0)].data.bytes()[-20:])

    rein_blocks = Partition((5, 6, 7), name='rein')
    out_blocks.repartition(rein_blocks, None, keep.keep)
    rein_blocks.blocks[(0, 0, 0)].read()

    assert(array.blocks[(0, 0, 0)].data.bytes() ==
           rein_blocks.blocks[(0, 0, 0)].data.bytes())


def test_repartition_keep_2(cleanup_blocks):
    array = Partition((10, 20, 30), name='array')
    in_blocks = Partition((10, 20, 30), name='in', array=array, fill='random')
    in_data = in_blocks.blocks[(0, 0, 0)].data.bytes()
    in_blocks.write()
    out_blocks = Partition((10, 10, 15), name='out', array=array)
    in_blocks.repartition(out_blocks, None, keep.keep)

    rein_blocks = Partition((10, 20, 30), name='rein', array=array)
    out_blocks.repartition(rein_blocks, None, keep.keep)
    rein_blocks.blocks[(0, 0, 0)].read()
    rein_data = rein_blocks.blocks[(0, 0, 0)].data.bytes()

    assert(rein_data == in_data)


def test_repartition_baseline_3(cleanup_blocks):
    array = Partition((12, 12, 12), name='array', fill='random')
    array.write()
    in_blocks = Partition((4, 4, 4), name='in', array=array)
    array.repartition(in_blocks, None, keep.baseline)

    out_blocks = Partition((3, 3, 3), name='out', array=array)
    in_blocks.repartition(out_blocks, None, keep.baseline)

    rein_blocks = Partition((12, 12, 12), name='rein', array=array)
    out_blocks.repartition(rein_blocks, None, keep.baseline)

    rein_blocks.blocks[(0, 0, 0)].read()
    rein_data = rein_blocks.blocks[(0, 0, 0)].data.bytes()

    array.blocks[(0, 0, 0)].read()
    array_data = array.blocks[(0, 0, 0)].data.bytes()

    assert(rein_data == array_data)


def test_repartition_keep_3(cleanup_blocks):
    array = Partition((6, 6, 6), name='array', fill='random')
    array.write()
    array.clear()
    in_blocks = Partition((3, 3, 3), name='in', array=array)
    array.repartition(in_blocks, None, keep.keep)

    in_blocks.clear()
    out_blocks = Partition((2, 2, 2), name='out', array=array)
    in_blocks.repartition(out_blocks, None, keep.keep)

    array = Partition((12, 12, 12), name='array', fill='random')
    array.write()
    array.clear()
    in_blocks = Partition((4, 4, 4), name='in', array=array)
    array.repartition(in_blocks, None, keep.keep)

    in_blocks.clear()
    out_blocks = Partition((3, 3, 3), name='out', array=array)
    in_blocks.repartition(out_blocks, None, keep.keep)

    rein_blocks = Partition((12, 12, 12), name='rein', array=array)
    out_blocks.repartition(rein_blocks, None, keep.keep)

    rein_blocks.blocks[(0, 0, 0)].read()
    rein_data = rein_blocks.blocks[(0, 0, 0)].data.bytes()

    array.blocks[(0, 0, 0)].read()
    array_data = array.blocks[(0, 0, 0)].data.bytes()

    assert(rein_data == array_data)


def test_partition_clear(cleanup_blocks):
    array = Partition((12, 12, 12), name='array', fill='random')
    array.clear()
