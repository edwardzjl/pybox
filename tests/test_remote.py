from pybox import RemotePyboxManager


def test_ws_url():
    manager = RemotePyboxManager("http://example.com")
    ws_url = manager.get_ws_url("foo")
    assert ws_url == "ws://example.com/api/kernels/foo/channels"
