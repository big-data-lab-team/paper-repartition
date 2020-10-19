# Keep

[![Build Status](https://travis-ci.org/big-data-lab-team/paper-repartition.svg?branch=master)](https://travis-ci.org/big-data-lab-team/paper-repartition)  [![codecov](https://codecov.io/gh/big-data-lab-team/paper-repartition/branch/master/graph/badge.svg?token=4uLZEJn73Y)](https://codecov.io/gh/big-data-lab-team/paper-repartition/branch/master)

Implementation of the keep heuristic described in the paper.

For usage, see ```python repartition.py --help```

Example:
```
python repartition.py --max-mem 5000000 --create --delete --test-data '(500, 500, 500)' '(50, 50, 50)' '(100, 100, 100)' keep
```