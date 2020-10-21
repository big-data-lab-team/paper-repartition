import glob
import pytest
import os
from keep.repartition import main


@pytest.fixture
def cleanup_blocks():
    yield
    for f in glob.glob("*.bin"):
        os.remove(f)


def test_repartition(cleanup_blocks):
    main(["--create", "(50, 50, 50)", "(5, 5, 5)", "(10, 10, 10)", "keep"])

    # verify that no output blocks have been created
    assert len(glob.glob("out*.bin")) == 0
    main(
        ["--repartition", "(50, 50, 50)", "(5, 5, 5)", "(10, 10, 10)", "keep"]
    )

    # verify that output blocks have been created
    assert len(glob.glob("out*.bin")) > 0

    main(["--test-data", "(50, 50, 50)", "(5, 5, 5)", "(10, 10, 10)", "keep"])

    main(["--delete", "(50, 50, 50)", "(5, 5, 5)", "(10, 10, 10)", "keep"])

    # verify that all output blocks have been removed
    assert len(glob.glob("out*.bin")) == 0
