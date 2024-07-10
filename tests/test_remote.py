from pybox import RemotePyBoxManager


def test_ws_url():
    manager = RemotePyBoxManager("http://example.com")
    ws_url = manager.get_ws_url("foo")
    assert ws_url == "ws://example.com/api/kernels/foo/channels"
