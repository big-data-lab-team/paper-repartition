import glob
import pytest
import os
from keep.repartition import main


@pytest.fixture
def cleanup_blocks():
    yield
    for f in glob.glob('*.bin'):
        os.remove(f)


def test_repartition(cleanup_blocks):
    main(['--create',
          '--delete',
          '--test-data',
          '(50, 50, 50)',
          '(5, 5, 5)',
          '(10, 10, 10)',
          'keep'])
