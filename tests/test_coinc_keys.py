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
    assert not hasattr(window, "_sync_gui_from_workflow")
    assert not hasattr(window, "_sync_status_from_workflow")

    assert window.workflow.raw is not None
    assert window.workflow.current is not None
    assert not hasattr(window, "data_raw")
    assert not hasattr(window, "data_current")
    assert calls["count"] == 1


def test_workflow_is_the_only_data_state():
    window = MainWindow()
    raw = np.array([[1, 2, 3], [4, 5, 6]])
    window.workflow.set_loaded_data(raw, "2E1P")

    assert window.workflow.raw is not None
    assert window.workflow.postproc is not None
    assert window.workflow.calibrated is not None
    assert window.workflow.current is not None
    assert window.workflow.raw is window.workflow.postproc or window.workflow.postproc.shape == window.workflow.raw.shape
    assert window.workflow.raw is window.workflow.calibrated or window.workflow.calibrated.shape == window.workflow.raw.shape
    assert window.workflow.raw is window.workflow.current or window.workflow.current.shape == window.workflow.raw.shape

    assert not hasattr(window, "data_raw")
    assert not hasattr(window, "data_postproc")
    assert not hasattr(window, "data_calibrated")
    assert not hasattr(window, "data_current")
