import glob
import math
import os
import pytest
from keep.block import Block


@pytest.fixture
def cleanup_blocks():
    yield
    for f in glob.glob('*.bin'):
        os.remove(f)


def test_block_offsets():
    b = Block((1, 2, 3), (5, 6, 7))
    assert((b.block_offsets(b)) == (b.origin, b.shape,
                                    (0, 209)))
    c = Block((0, 0, 0), (4, 4, 4))
    assert(c.block_offsets(b) == (b.origin, (3, 2, 1),
                                  (27,
                                   27,
                                   31,
                                   31,
                                   43,
                                   43,
                                   47,
                                   47,
                                   59,
                                   59,
                                   63,
                                   63)))
    d = Block((1, 2, 2), (4, 4, 4))
    assert(c.block_offsets(d) == (d.origin,
                                  (3, 2, 2),
                                  (26,
                                   27,
                                   30,
                                   31,
                                   42,
                                   43,
                                   46,
                                   47,
                                   58,
                                   59,
                                   62,
                                   63)))
    e = Block((1, 2, 1), (4, 4, 4))
    assert(c.block_offsets(e) == (e.origin,
                                  (3, 2, 3),
                                  (25,
                                   27,
                                   29,
                                   31,
                                   41,
                                   43,
                                   45,
                                   47,
                                   57,
                                   59,
                                   61,
                                   63)))


def test_delete():
    file_name = 'test.bin'
    b = Block((1, 1, 1), (4, 5, 6), fill='zeros', file_name=file_name)
    assert(os.path.isfile(file_name))
    b.delete()
    assert(not os.path.isfile(file_name))


def test_read_from_disjoint():
    b = Block((4, 5, 6), (2, 4, 6))
    c = Block((40, 50, 60), (2, 4, 6))
    assert(b.read_from(c) == (0, 0))


def test_read_from_shape_match(cleanup_blocks):
    b = Block((1, 2, 3), (5, 6, 7), fill='random', file_name='test.bin')
    b.clear()

    by, _, _ = b.read_from(b)
    assert(by == math.prod(b.shape))

    with open(b.file_name, 'rb') as f:
        data = f.read()
    assert(data == b.data.bytes())
    os.remove(b.file_name)


def test_read_from_shape_mismatch(cleanup_blocks):
    b = Block((1, 2, 3), (5, 6, 7), file_name='test.bin')
    c = Block((1, 2, 3), (5, 2, 7), fill='random', file_name='block1.bin')
    d = Block((1, 4, 3), (5, 4, 7), fill='random', file_name='block2.bin')
    l, _, _ = b.read_from(c)
    m, _, _ = b.read_from(d)
    assert(l + m == math.prod(b.shape))

    # Check content
    c.read()
    b.read()
    d.read()
    assert(c.data.bytes()[:10] == b.data.bytes()[:10])
    assert(d.data.bytes()[-10:] == b.data.bytes()[-10:])
    for fn in (c.file_name, d.file_name):
        os.remove(fn)


def test_offset():
    b = Block((1, 1, 1), (4, 5, 6))
    assert(b.offset((1, 1, 3)) == 2)
    assert(b.offset((2, 2, 2)) == 1+6+30)

    c = Block((1, 2, 3), (5, 6, 7))
    assert(c.offset((1, 2, 9)) == 6)
    assert(c.offset((1, 7, 3)) == 35)
    assert(c.offset((1, 7, 9)) == 41)
    assert(c.offset((2, 2, 3)) == 42)
    assert(c.offset((5, 7, 9)) == math.prod(c.shape)-1)


def test_write_to_shape_match(cleanup_blocks):

    b = Block((1, 2, 3), (5, 6, 7), fill='random', file_name='test.bin')
    b.read()
    by, _, _ = b.write_to(b)
    assert(by == math.prod(b.shape))

    with open(b.file_name, 'rb') as f:
        data = f.read()
    assert(data == b.data.bytes())
    os.remove(b.file_name)


def test_zeros(cleanup_blocks):
    b = Block((4, 5, 6), (2, 4, 6), fill='zeros', file_name='block.bin')
    b.read()
    assert(b.data.bytes() == bytearray(math.prod(b.shape)))


def test_write_to_shape_mismatch(cleanup_blocks):
    b = Block((1, 2, 3), (5, 6, 7), fill='random', file_name='test.bin')
    c = Block((1, 2, 3), (5, 2, 7), file_name='block1.bin')
    d = Block((1, 4, 3), (5, 4, 7), file_name='block2.bin')
    b.read()
    l, _, _ = b.write_to(c)
    m, _, _ = b.write_to(d)
    assert(l + m == math.prod(b.shape))

    # Check content
    c.read()
    assert(c.data.bytes()[:10] == b.data.bytes()[:10])
    d.read()
    assert(d.data.bytes()[-10:] == b.data.bytes()[-10:])
    for fn in (c.file_name, d.file_name):
        os.remove(fn)


def test_write_to_read_from(cleanup_blocks):
    b = Block((1, 2, 3), (5, 6, 7), fill='random', file_name='test.bin')
    c = Block((1, 2, 3), (5, 2, 7), file_name='block1.bin')
    d = Block((1, 4, 3), (5, 4, 7), file_name='block2.bin')
    b.read()
    l, _, _ = b.write_to(c)
    m, _, _ = b.write_to(d)

    original_data = bytearray()
    original_data[:] = b.data.bytes()
    b.clear()
    b.read_from(c)
    b.read_from(d)
    assert(b.data.bytes() == original_data)
    for fn in (c.file_name, d.file_name):
        os.remove(fn)
