from metro_eval.coinc.gui_pyside import MainWindow


def test_ep_number_from_string_handles_common_keys():
    assert MainWindow.EP_number_from_string("E") == (1, 0)
    assert MainWindow.EP_number_from_string("EE") == (2, 0)
    assert MainWindow.EP_number_from_string("P") == (0, 1)
    assert MainWindow.EP_number_from_string("1P") == (0, 1)
    assert MainWindow.EP_number_from_string("2E") == (2, 0)
    assert MainWindow.EP_number_from_string("2E1P") == (2, 1)
    assert MainWindow.EP_number_from_string("EP") == (1, 1)
    assert MainWindow.EP_number_from_string("EEP") == (2, 1)

