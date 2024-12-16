# Quickstart

## Installation

You can easily install PyBox using pip:

```sh
python -m pip install pppybox
```

PyBox supports two types of Python sandbox environments: remote and local.

- Remote Sandbox: Requires access to a running [Enterprise Gateway](https://github.com/jupyter-server/enterprise_gateway) instance.
- Local Sandbox: Requires additional dependencies, which can be installed with the following command:

    ```sh
    python -m pip install pppybox[local]
    ```

I recommend start from local sandbox and migrate to the remote sandbox when you are ready.

## Spawning Local Sandboxes

To spawn local sandboxes, first create a `PyBoxManager` instance:

```python
from pybox import LocalPyBoxManager

pybox_manager = LocalPyBoxManager()
```

### With Context Manager

Then, use a context manager to spawn and interact with a local sandbox:

```python
with pybox_manager.start() as box:
    box.execute("print('Hello, World!')")
```

> It's recommended to use a context manager to ensure that the sandbox is properly cleaned up after use.

### Without Context Manager

```python
box = pybox_manager.start()
...
pybox_manager.shutdown(box.kernel_id, now=True)
```
