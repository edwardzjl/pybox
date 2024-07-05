from pybox.utils import clean_ansi_codes


def test_no_ansi_codes():
    assert clean_ansi_codes("Hello, World!") == "Hello, World!"


def test_with_ansi_codes():
    assert clean_ansi_codes("\x1b[31mHello, World!\x1b[0m") == "Hello, World!"


def test_multiple_ansi_codes():
    assert clean_ansi_codes("\x1b[31mHello\x1b[0m, \x1b[32mWorld\x1b[0m!") == "Hello, World!"


def test_empty_string():
    assert clean_ansi_codes("") == ""


def test_only_ansi_codes():
    assert clean_ansi_codes("\x1b[31m\x1b[0m") == ""


def test_mixed_content():
    assert (
        clean_ansi_codes("Text\x1b[31m with \x1b[0mmultiple \x1b[32mANSI\x1b[0m codes")
        == "Text with multiple ANSI codes"
    )


def test_unicode_characters():
    assert clean_ansi_codes("Hello, \u001b[31m世界\u001b[0m!") == "Hello, 世界!"


def test_extended_ansi_codes():
    assert clean_ansi_codes("\u001b[38;5;82mGreen text\u001b[0m") == "Green text"


def test_invalid_ansi_codes():
    assert clean_ansi_codes("\x1b[31mHello, World!") == "Hello, World!"
    assert clean_ansi_codes("Hello, World!\x1b[31m") == "Hello, World!"
