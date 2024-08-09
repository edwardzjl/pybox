from uuid import uuid4

import pytest
from pybox.kube import KubePyBoxManager


@pytest.fixture(scope="module")
def kube_manager() -> KubePyBoxManager:
    return KubePyBoxManager(
        incluster=False,
        kernel_env={
            "KERNEL_USERNAME": "tablegpt",
            "KERNEL_NAMESPACE": "default",
            "KERNEL_IMAGE": "zjuici/tablegpt-kernel:0.1.1",
            "KERNEL_WORKING_DIR": "/mnt/data",
            "KERNEL_VOLUME_MOUNTS": [
                {"name": "shared-vol", "mountPath": "/mnt/data"},
                {"name": "ipython-profile-vol", "mountPath": "/opt/startup"},
                {
                    "name": "kernel-launch-vol",
                    "mountPath": "/usr/local/bin/bootstrap-kernel.sh",
                    "subPath": "bootstrap-kernel.sh",
                },
                {
                    "name": "kernel-launch-vol",
                    "mountPath": "/usr/local/bin/kernel-launchers/python/scripts/launch_ipykernel.py",
                    "subPath": "launch_ipykernel.py",
                },
            ],
            "KERNEL_VOLUMES": [
                {
                    "name": "shared-vol",
                    "nfs": {
                        "server": "10.0.0.29",
                        "path": "/data/tablegpt-slim-py/data",
                    },
                },
                {
                    "name": "ipython-profile-vol",
                    "configMap": {"name": "ipython-startup-scripts"},
                },
                {
                    "name": "kernel-launch-vol",
                    "configMap": {
                        "defaultMode": 0o755,
                        "name": "kernel-launch-scripts",
                    },
                },
            ],
            "KERNEL_STARTUP_SCRIPTS_PATH": "/opt/startup",
            "KERNEL_IDLE_TIMEOUT": "1800",
        },
    )


@pytest.mark.skip(reason="Start kernel cr need kubernetes environment")
def test_start_with_user(kube_manager: KubePyBoxManager) -> None:
    kernel_id = str(uuid4())
    box = kube_manager.start(
        kernel_id=kernel_id,
        cwd="/mnt/data",
        username="dev",
    )
    assert box.kernel_id == kernel_id


@pytest.mark.skip(reason="Start kernel cr need kubernetes environment")
def test_start_without_user(kube_manager: KubePyBoxManager) -> None:
    kernel_id = str(uuid4())
    box = kube_manager.start(
        kernel_id=kernel_id,
        cwd="/mnt/data",
    )
    assert box.kernel_id == kernel_id


@pytest.mark.skip(reason="Start kernel cr need kubernetes environment")
@pytest.mark.asyncio
async def test_start_async(kube_manager: KubePyBoxManager) -> None:
    kernel_id = str(uuid4())
    box = await kube_manager.astart(
        kernel_id=kernel_id,
        cwd="/mnt/data",
    )
    assert box.kernel_id == kernel_id


@pytest.mark.skip(reason="Shutting down kernel cr need kubernetes environment")
def test_shutdown_w_id(kube_manager: KubePyBoxManager) -> None:
    kube_manager.shutdown(kernel_id="1918a836-e941-4332-9e6f-dbfe91e5771a")


@pytest.mark.skip(reason="Shutting down kernel cr need kubernetes environment")
@pytest.mark.asyncio
async def test_shutdown_async(kube_manager: KubePyBoxManager) -> None:
    await kube_manager.ashutdown(kernel_id="1918a836-e941-4332-9e6f-dbfe91e5771a")


if __name__ == "__main__":
    pytest.main()
