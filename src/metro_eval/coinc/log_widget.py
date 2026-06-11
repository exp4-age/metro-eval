
from PySide6.QtCore import (QObject, Signal)
import logging
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QTextEdit
)

# =============================
# Logging
# =============================
class LogEmitter(QObject):

    message_received = Signal(str)

class QtLogHandler(logging.Handler):

    def __init__(self, emitter):
        super().__init__()

        self.emitter = emitter

    def emit(self, record):

        msg = self.format(record)

        self.emitter.message_received.emit(msg)

class LogWidget(QWidget):

    def __init__(self):
        super().__init__()

        layout = QVBoxLayout(self)

        self.text_edit = QTextEdit()
        self.text_edit.setReadOnly(True)

        layout.addWidget(self.text_edit)

    def append_message(self, message):

        self.text_edit.append(message)

        scrollbar = self.text_edit.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())




def setup_gui_logging(log_widget):

    emitter = LogEmitter()

    emitter.message_received.connect(
        log_widget.append_message
    )

    handler = QtLogHandler(emitter)

    formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S"
    )

    handler.setFormatter(formatter)

    root_logger = logging.getLogger()

    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)

    return handler