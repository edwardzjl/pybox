# PyBox

[![PyPI - Version](https://img.shields.io/pypi/v/pppybox.svg)](https://pypi.org/project/pppybox)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pppybox.svg)](https://pypi.org/project/pppybox)

-----

PyBox manages Python code sandboxes and provides a clean interface for interacting with them. PyBox supports spawning sandboxes in both local and remote environments.

There are two main abstractions, `pybox.base.BasePyBoxManager` and `pybox.base.BasePyBox`

## Quickstart

### Installation

To install PyBox:

```sh
python -m pip install pppybox
```

Note that local sandbox requires additional dependencies. To install them, run:

```sh
python -m pip install pppybox[local]
```

## License

`pybox` is distributed under the terms of the [Apache 2.0](https://spdx.org/licenses/Apache-2.0.html) license.
