import traceback, sys
import time
import random
from random_word import RandomWords

from appui import Ui_MainWindow
from PyQt5.QtGui import *
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *

WORDS = RandomWords()

lock = QReadWriteLock()


class Client(object):
    def __init__(self, name: str):
        self.name = name
        self.files = []
        self.priority = 0.0

        # Variable describing when client was added to queue
        self.time_added = time.time()

        # Variable describing when was the last time this client's file was handled
        self.last_handle = time.time()

        self.generate_files()

    def generate_files(self):
        for _ in range(random.randint(1, 15)):
            self.files.append(ClientFile(self, random.randint(1, 1024)))

        self.files.sort(key=lambda client_file: client_file.size)


class ClientFile(object):
    def __init__(self, client: Client, file_size: int):
        self.client_id = id(client)
        self.size = file_size


class WorkerSignals(QObject):
    # General signals
    finished = pyqtSignal()
    error = pyqtSignal(tuple)
    result = pyqtSignal(object)
    progress = pyqtSignal(int)

    # Disk signals
    disk_thread_started = pyqtSignal(int, str, ClientFile)
    disk_progress = pyqtSignal(int, int)


class Worker(QRunnable):
    def __init__(self, fn, *args, **kwargs):
        super(Worker, self).__init__()

        # Store constructor arguments
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.signals = WorkerSignals()

        # Add the callback to our kwargs
        if "disk_worker" in self.kwargs:
            self.kwargs["progress_callback"] = self.signals.disk_progress

    @pyqtSlot()
    def run(self):
        try:
            # Inform that disk thread has started
            if "disk_worker" in self.kwargs:
                self.signals.disk_thread_started.emit(
                    self.kwargs["disk_index"],
                    self.kwargs["client_name"],
                    self.kwargs["client_file"],
                )

            # Run thread function
            result = self.fn(*self.args, **self.kwargs)
        except:
            traceback.print_exc()
            exctype, value = sys.exc_info()[:2]
            self.signals.error.emit((exctype, value, traceback.format_exc()))
        else:
            # Return the result of the processing
            self.signals.result.emit(result)
        finally:
            # Done
            self.signals.finished.emit()


class MainWindow(QMainWindow, Ui_MainWindow):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Init all the ui elements
        self.setupUi(self)

        # Init disk threads
        self.disk_threads = {
            1: None,
            2: None,
            3: None,
            4: None,
            5: None,
        }

        # Init clients
        self.clients = {}

        # Init Table
        self.table_of_clients = QTableWidget()
        self.table_of_clients.setColumnCount(2)
        self.table_of_clients.setHorizontalHeaderLabels(["Client Name", "Client Files"])
        header = self.table_of_clients.horizontalHeader()
        header.setSectionResizeMode(1, QHeaderView.Stretch)
        self.horizontal_layout.addWidget(self.table_of_clients)

        # Connect buttons to functions
        self.button_add_client.clicked.connect(self.add_client)
        self.button_start.clicked.connect(self.start_load_balancer)
        self.button_stop.clicked.connect(self.stop_disk_threads)

        # Init thread pool
        self.threadpool = QThreadPool()
        print(
            "Multithreading with maximum %d threads" % self.threadpool.maxThreadCount()
        )

    def thread_complete(self):
        print("Thread complete")

    def disk_thread_started(
        self, disk_index: int, client_name: str, client_file: ClientFile
    ):
        print(
            f"Disk {disk_index} started. Owner: {client_name} | File name: {client_file.name} | File size: {client_file.size}"
        )

        if disk_index == 1:
            self.label_disk1.setText(
                f"Owner: {client_name} | File size: {client_file.size}"
            )

        if disk_index == 2:
            self.label_disk2.setText(
                f"Owner: {client_name} | File size: {client_file.size}"
            )

        if disk_index == 3:
            self.label_disk3.setText(
                f"Owner: {client_name} | File size: {client_file.size}"
            )

        if disk_index == 4:
            self.label_disk4.setText(
                f"Owner: {client_name} | File size: {client_file.size}"
            )

        if disk_index == 5:
            self.label_disk5.setText(
                f"Owner: {client_name} | File size: {client_file.size}"
            )

    def disk_progress(self, disk_index: int, percentage: int):
        if disk_index == 1:
            self.progress_bar_disk1.setValue(percentage)
            return

        if disk_index == 2:
            self.progress_bar_disk2.setValue(percentage)
            return

        if disk_index == 3:
            self.progress_bar_disk3.setValue(percentage)
            return

        if disk_index == 4:
            self.progress_bar_disk4.setValue(percentage)
            return

        if disk_index == 5:
            self.progress_bar_disk5.setValue(percentage)
            return

    def disk_task(self, **kwargs):
        disk_index = kwargs["disk_index"]
        client_name = kwargs["client_name"]
        client_file = kwargs["client_file"]
        progress_callback = kwargs["progress_callback"]

        for i in range(client_file.size):
            time.sleep(1)
            progress_callback.emit(disk_index, int(i * 100 / client_file.size))

        progress_callback.emit(disk_index, 100)

    def disk_thread_complete(self, disk_index):
        print("Disk thread complete")
        self.disk_threads[disk_index] = None
        print(self.disk_threads)

    def start_load_balancer(self):
        """
        For each 'free' disk launch auction
        """
        
        first_client = next(iter(self.clients.items()))
        client_file = first_client[1].files.pop(0)
        first_client[1].table_widget.removeRow(0)

        if first_client[1].table_widget.item(0, 0) is None:
            first_client[1].table_widget.deleteLater()
            self.clients.pop(first_client[0])

        self.disk_threads[1] = Worker(
            self.disk_task,
            disk_index=1,
            client_name=first_client[1].name,
            client_file=client_file,
            disk_worker=True,
        )
        self.disk_threads[1].signals.disk_thread_started.connect(
            self.disk_thread_started
        )
        self.disk_threads[1].signals.disk_progress.connect(self.disk_progress)
        self.disk_threads[1].signals.finished.connect(self.thread_complete)

        self.threadpool.start(self.disk_threads[1])

    def stop_disk_threads(self):
        for thread in self.disk_threads.values():
            thread.stop()

        self.button_start.setEnabled(True)

    def add_client_to_table(self, client):
        # Add client to QT table
        rowPosition = self.table_of_clients.rowCount()
        self.table_of_clients.insertRow(rowPosition)
        self.table_of_clients.setItem(rowPosition, 0, QTableWidgetItem(client.name))
        self.table_of_clients.setItem(
            rowPosition,
            1,
            QTableWidgetItem(
                " | ".join([str(client_file.size) for client_file in client.files])
            ),
        )

        # Add client to dictionary
        lock.lockForWrite()
        self.clients[id(client)] = client
        lock.unlock()

    def generate_client(self):
        return Client(name=WORDS.get_random_word())

    def add_client(self):
        worker = Worker(self.generate_client)
        worker.signals.result.connect(self.add_client_to_table)
        worker.signals.finished.connect(self.thread_complete)
        self.threadpool.start(worker)


if __name__ == "__main__":
    app = QApplication(sys.argv)

    window = MainWindow()
    window.show()

    app.exec_()
