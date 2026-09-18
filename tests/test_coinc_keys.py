import numpy as np

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


def test_sync_gui_from_workflow_does_not_rebuild_ui(monkeypatch):
    calls = {"count": 0}

    def fake_build(self):
        calls["count"] += 1

    monkeypatch.setattr(MainWindow, "build_ui", fake_build)

    window = MainWindow()
    assert calls["count"] == 1

    window.workflow.set_loaded_data(np.array([[1, 2, 3], [4, 5, 6]]), "2E1P")
    window._sync_gui_from_workflow()

    assert window.data_raw is not None
    assert window.data_current is not None
    assert calls["count"] == 1
