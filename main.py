from PySide6.QtWidgets import (QMainWindow, QApplication, QTableView, QPushButton, 
                               QFileDialog, QTabWidget, QMessageBox, QMenuBar, 
                               QGroupBox, QVBoxLayout, QHBoxLayout, QCheckBox, QGridLayout,
                               QLineEdit, QMenu, QWidget, QToolButton, QSpacerItem, QSizePolicy,
                               QComboBox, QSplitter, QLabel)
from PySide6.QtCore import Qt, Slot, Signal, QRect
from PySide6 import QtWidgets, QtGui, QtCore
from PySide6.QtGui import QStandardItemModel, QStandardItem, QAction, QIcon, QShortcut, QKeySequence, QCursor, QColor, QFont
import pandas as pd
import logging
import sys
import os

from path import PathManager
from DataFrameDataJoin import myDataJoinFarame as mDJF

logging.basicConfig(level=logging.INFO, filename="log.log", filemode="w", format="%(asctime)s - %(lineno)d - %(levelname)s - %(message)s") #INFO NOTSET


class DataFrameTableModel(QStandardItemModel):
    def __init__(self, df=None, filename=''):
        super().__init__()
        self._df = df if df is not None else pd.DataFrame()
        self._filename = filename
        self._changes = [self._df.copy()]
        self._currChange = 0
        self.update_view()

        #self.itemChanged.connect(self.onItemChanged)

    def setDataFrame(self, df):
        self._df = df
        self._changes = [df.copy()]
        self._currChange = 0
        self.update_view()

    def update_view(self):
        self.clear()

        self.setHorizontalHeaderLabels(self._df.columns.tolist())

        self.setColumnCount(len(self._df.columns))
        self.setRowCount(len(self._df))
        for row in range(len(self._df)):
            for col in range(len(self._df.columns)):
                value = self._df.iat[row, col]

                if isinstance(value, float):
                    item = f"{value:.2f}"
                else:
                    item = str(value)
                self.setItem(row, col, QStandardItem(item))
    
    '''
    def addChange(self):
        """Track change in DataFrame."""
        self._currChange += 1
        #self._changes = self._changes[:self._currChange]
        #self._changes.append(self._df.copy())
        #print(self._currChange)
        #print(self._changes)
        #self.trackDataChange.emit()

    def onItemChanged(self, item):
        print("chenge")

        """Handle item change and update the DataFrame."""

        row = item.row()
        col = item.column()
        value = item.text()

        self._df.iat[row, col] = value

        self.addChange()
    '''


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.dark_mode_enabled = False
        # Główne okno
        self.setWindowTitle("Mój program")
        
        # Główne okno bez marginesów
        central_widget = QWidget(self)
        self.setCentralWidget(central_widget)

        # Główne układy poziomy i pionowy
        main_layout = QVBoxLayout()
        central_widget.setLayout(main_layout)

        # Tworzenie górnego układu przycisków i opcji
        top_layout = QHBoxLayout()

        fname = []
        font = QFont()
        font.setPointSize(8) 

        self.button_import = QPushButton("Import", self)
        self.button_import.setFixedHeight(26)  # Set fixed height
        self.button_import.setFixedWidth(65)   # Set fixed width
        self.button_import.clicked.connect(self.main_data_import)

        self.button_export = QPushButton("Export", self)
        self.button_export.setFixedHeight(26)  # Set fixed height
        self.button_export.setFixedWidth(65)   # Set fixed width
        self.button_export.clicked.connect(self.export)
        self.button_export.setToolTip('<p>Export do <b>EXCEL</b>.</p>')

        self.main_col = QtWidgets.QComboBox(self)
        my_main_col = ["Select column."]
        self.main_col.addItems(my_main_col)
        self.main_col.setMaxVisibleItems(30)
        self.main_col.setFont(font)
        self.main_col.setView(QtWidgets.QListView())
        self.main_col.setGeometry(1000, 2, 200, 26)

        self.main_heder = QCheckBox("Heder")
        self.main_heder.stateChanged.connect(lambda state: self.main_heders(True) if state == 2 else self.main_heders(False))


        self.button_merge = QPushButton("Merge")
        self.button_merge.setFixedHeight(26)  # Set fixed height
        self.button_merge.setFixedWidth(65)   # Set fixed width
        self.button_merge.clicked.connect(self.merging)

        self.button_join_import = QPushButton("import")
        self.button_join_import.clicked.connect(self.join_data_import)

        self.join_col = QtWidgets.QComboBox(self)
        my_join_col = ["Select column."]
        self.join_col.addItems(my_join_col)
        self.join_col.setMaxVisibleItems(30)
        self.join_col.setFont(font)
        self.join_col.setView(QtWidgets.QListView())
        self.join_col.setGeometry(1000, 2, 200, 26)


        self.join_heder = QCheckBox("Heder")
        self.join_heder.stateChanged.connect(lambda state: self.join_heders(True) if state == 2 else self.join_heders(False))

        self.combo_box_1 = QComboBox()
        self.combo_box_1.addItem("Łącz kolumną:")
        
        self.combo_box_2 = QComboBox()
        self.combo_box_2.addItem("Merge by column:")

        self.init_resize_UI()
        self.GMLDefaultTable()

    def init_resize_UI(self):
        central_widget = QWidget(self)
        self.setCentralWidget(central_widget)

        main_layout = QVBoxLayout(central_widget)
        main_layout.setContentsMargins(0, 2, 0, 0)  # Usuń marginesy
        main_layout.setSpacing(0)

        # Layout dla górnego paska przycisków
        button_layout = QHBoxLayout()
        button_layout.setSpacing(0)

        # Dodawanie przycisków i innych elementów do układu
        button_layout.addWidget(self.button_import)
        button_layout.addWidget(self.button_export)
        button_layout.addSpacerItem(QSpacerItem(3, 0, QSizePolicy.Fixed, QSizePolicy.Minimum))
        button_layout.addWidget(self.main_col)
        button_layout.addSpacerItem(QSpacerItem(3, 0, QSizePolicy.Fixed, QSizePolicy.Minimum))
        button_layout.addWidget(self.main_heder)
        button_layout.addStretch(1) 

        button_layout.addWidget(self.button_join_import)
        button_layout.addSpacerItem(QSpacerItem(3, 0, QSizePolicy.Fixed, QSizePolicy.Minimum))
        button_layout.addWidget(self.join_col)
        button_layout.addSpacerItem(QSpacerItem(3, 0, QSizePolicy.Fixed, QSizePolicy.Minimum))
        button_layout.addWidget(self.join_heder)
        button_layout.addWidget(self.button_merge)

        main_layout.addLayout(button_layout)

        # Zamiast QHBoxLayout dla tabel, używamy QSplitter
        table_splitter = QSplitter(Qt.Horizontal)  # QSplitter poziomy (horizontal)

        # Tabela główna
        self.main_table = QTableView(self)
        self.main_table.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)
        self.main_table.customContextMenuRequested.connect(self.context_menu)
        table_splitter.addWidget(self.main_table)

        # Tabela łącząca
        self.join_table = QTableView(self)
        self.join_table.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)
        self.join_table.customContextMenuRequested.connect(self.context_menu)
        table_splitter.addWidget(self.join_table)

        # Ustaw proporcje domyślne (2:1)
        table_splitter.setStretchFactor(0, 2)  # Tabela 1 - 2/3 szerokości
        table_splitter.setStretchFactor(1, 1)  # Tabela 2 - 1/3 szerokości

        # Dodaj QSplitter do głównego układu
        main_layout.addWidget(table_splitter)

        # Ustaw minimalny rozmiar okna
        self.setMinimumSize(800, 600)


    def main_data_import(self, path):
        filters = "TXT Files (*.txt);;CSV Files (*.csv);;Excel Files (*.xlsx)"
        if not path:
            path, _ = QFileDialog.getOpenFileName(self, "Select a file", os.path.expanduser("~/Desktop"), filters)

        if not path:
            return
        
        mDJF.read("df_main", path)

        self.display_data()

    def main_heders(self, state):
        if state:
            mDJF.df_header("df_main", True)
        else:
            mDJF.df_header("df_main", False)
        self.display_data()

    def join_data_import(self, path):
        filters = "TXT Files (*.txt);;CSV Files (*.csv);;Excel Files (*.xlsx)"
        if not path:
            path, _ = QFileDialog.getOpenFileName(self, "Select a file", os.path.expanduser("~/Desktop"), filters)

        if not path:
            return
        
        mDJF.read("df_data", path)

        self.display_data()

    def join_heders(self, state):
        if state:
            mDJF.df_header("df_data", True)
        else:
            mDJF.df_header("df_data", False)
        self.display_data()

    def export(self):
        s_name = QFileDialog.getSaveFileName(self, 'select a file', os.path.expanduser("~/Desktop"),'Excel File(*.xlsx);;TXT File (*.txt);;TXT File With Tab Separator (*.txt);;CSV File (*.csv)')
        
        if s_name == ('', ''):
            return

        df = mDJF.df_main
 
        if s_name[1] == "Excel File(*.xlsx)":
            df.to_excel(s_name[0], index=False)
        
        if s_name[1] == "TXT File (*.txt)":
            df.to_csv(s_name[0], index=False, sep=' ')

        elif s_name[1] == "TXT File With Tab Separator (*.txt)":
            df.to_csv(s_name[0], index=False, sep='\t')

        elif s_name[1] == "CSV File (*.csv)":
            df.to_csv(s_name[0], index=False)
        else:
            df.to_excel(s_name[0], index=False)

        print("Export")

    def merging(self):
        mDJF.df_main_base_col_name = self.main_col.currentText()
        mDJF.df_data_base_col_name = self.join_col.currentText()
        mDJF.main_base_merge_coll(self.main_col.currentText())
        mDJF.df_connexion(True)
        self.display_data()

    def display_data(self):
        df_main = mDJF.df_main
        df_main = df_main.fillna("")
        main_model = DataFrameTableModel(df_main)
        self.main_table.setModel(main_model)
        self.main_table.resizeColumnsToContents()

        if not mDJF.df_main.empty:
            last_selected_index = self.main_col.currentIndex()
            
            # Clear the existing items and populate with new columns
            self.main_col.clear()
            self.main_col.addItems(df_main.columns.tolist())
            
            # Check if the previously selected index is valid and restore it
            if 0 <= last_selected_index < self.main_col.count():
                self.main_col.setCurrentIndex(last_selected_index)
            else:
                # Optionally, handle cases where the previous index is out of bounds
                self.main_col.setCurrentIndex(0)

        df_join = mDJF.df_data
        df_join = df_join.fillna("")
        join_model = DataFrameTableModel(df_join)
        self.join_table.setModel(join_model)
        self.join_table.resizeColumnsToContents()

        if not mDJF.df_data.empty:
            self.join_col.clear()
            self.join_col.addItems(df_join.columns.tolist())


    def dragEnterEvent(self, event):
        if event.mimeData().hasUrls():
            urls = event.mimeData().urls()
            for url in urls:
                
                if url.toLocalFile().endswith(".txt") or url.toLocalFile().endswith(".csv") or url.toLocalFile().endswith(".xlsx"):
                    event.acceptProposedAction()
                    return       

    def dropEvent(self, event):
        for url in event.mimeData().urls():
            file_path = url.toLocalFile()
            mouse_pos = event.position().toPoint()  # Pobierz współrzędne myszy podczas upuszczania pliku

            # Sprawdź, do której tabeli należy współrzędna myszy
            if self.main_table.geometry().contains(mouse_pos):
                self.main_data_import(file_path)
            elif self.join_table.geometry().contains(mouse_pos):
                self.join_data_import(file_path)

    def copy_to_clipboard(self):
        selection_model = self.table_widget.selectionModel()

        selected_indexes = selection_model.selectedIndexes()

        if not selected_indexes:
            print("No items selected.")
            return

        selected_indexes.sort(key=lambda x: (x.row(), x.column()))
        
        copied_data = ''
        previous_row = selected_indexes[0].row()
        for index in selected_indexes:
            if index.row() != previous_row:
                copied_data += '\n'
                previous_row = index.row()

            copied_data += self.table_widget.model().data(index)
            if index.column() < self.table_widget.model().columnCount() - 1:
                copied_data += '\t'  # Tab delimiter between columns

        clipboard = QApplication.clipboard()
        clipboard.setText(copied_data)

        print("copy")

    def context_menu(self, pos):
        context_menu = QMenu(self)
        copy_action = context_menu.addAction("Kopiuj")
        copy_action.triggered.connect(self.copy_to_clipboard)
        context_menu.exec(QCursor.pos())


    def GMLDefaultTable(self):
        mDJF.initialize()
        self.display_data()


if __name__ == "__main__":
    app = QApplication(sys.argv)

    window = MainWindow()
    window.show()

    sys.exit(app.exec())
