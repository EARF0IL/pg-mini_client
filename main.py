import sys
import psycopg2
from psycopg2 import sql
import traceback

from PyQt5.QtWidgets import *
from PyQt5 import uic


class MainWin(QMainWindow):
    table_name: str
    db_conn = None
    columns = list()
    can_change = False

    def __init__(self):
        super().__init__()
        uic.loadUi("main.ui", self)
        self._initUI()

    def _initUI(self):
        self.setFixedSize(1500, 800)
        self.db_connect_button.clicked.connect(self.connect_db)
        self.select_button.clicked.connect(self.select_table)
        self.tableWidget.cellChanged.connect(self.update_db)
        self.tableWidget.cellDoubleClicked.connect(self.accept_changing)

    def _validate_connection_data(self):
        name = self.db_name_lineEdit.text()
        user = self.db_user_lineEdit.text()
        password = self.db_password_lineEdit.text()
        host = self.db_host_lineEdit.text()
        if not name or not user or not host:
            return -1
        return {'name': name, 'user': user, 'password': password, 'host': host}

    def _draw_table(self, data):
        for row in range(len(data)):
            for col in range(len(data[row])):
                self.tableWidget.setItem(row, col, QTableWidgetItem(str(data[row][col])))

    def connect_db(self):
        connection_data = self._validate_connection_data()
        if connection_data == -1:
            return
        success_connect = True
        message_box_text = "Successfully connect"
        try:
            if self.db_conn is not None:
                self.db_conn.close()
            self.db_conn = psycopg2.connect(dbname=connection_data['name'], user=connection_data['user'],
                                            password=connection_data['password'], host=connection_data['host'])
        except Exception as ex:
            success_connect = False
            # message_box_text = "Unknown error while connecting"
            message_box_text = traceback.format_exc()
        finally:
            QMessageBox.about(self, "Info", message_box_text)
        if not success_connect:
            return
        cur = self.db_conn.cursor()
        cur.execute("""SELECT tablename FROM pg_catalog.pg_tables 
                    WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'
                    AND tableowner = %s""", (connection_data['user'], ))
        table_names = list(map(lambda x: x[0], cur.fetchall()))
        self.table_select.clear()
        self.table_select.addItems(table_names)
        self.select_table()
        self.db_conn.commit()
        cur.close()

    def select_table(self):
        if self.db_conn is None or self.db_conn.closed:
            return
        self.can_change = False
        self.table_name = self.table_select.currentText()
        column = self.tableWidget.currentColumn()
        filter_expression = self.filter_lineEdit.text()
        cur = self.db_conn.cursor()
        cur.execute("""SELECT column_name FROM information_schema.columns WHERE table_name = %s""", (self.table_name, ))
        self.columns = list(map(lambda x: x[0], cur.fetchall()))
        if filter_expression:
            query = sql.SQL("""SELECT * FROM {} WHERE {} LIKE {}""").format(sql.Identifier(self.table_name),
                                                                            sql.Identifier(self.columns[column]),
                                                                            sql.Literal(filter_expression))
        else:
            query = sql.SQL("""SELECT * FROM {}""").format(sql.Identifier(self.table_name))
        cur.execute(query)
        data = cur.fetchall()
        data.sort()
        # print(columns)
        self.tableWidget.setColumnCount(len(self.columns))  # Set three columns
        self.tableWidget.setRowCount(len(data))
        self.tableWidget.setHorizontalHeaderLabels(self.columns)
        # self.tableWidget.horizontalHeaderItem(0).setToolTip("Column 1 ")
        # self.tableWidget.horizontalHeaderItem(1).setToolTip("Column 2 ")
        # self.tableWidget.horizontalHeaderItem(2).setToolTip("Column 3 ")
        self._draw_table(data)
        self.db_conn.commit()
        cur.close()

    def update_db(self, row, col):
        if not self.can_change:
            return
        id_number = self.tableWidget.item(row, 0).text()
        col_name = self.columns[col]
        current_text = self.tableWidget.item(row, col).text()
        cur = self.db_conn.cursor()
        query = sql.SQL("""UPDATE {} SET {} = {} WHERE id = {}""").format(sql.Identifier(self.table_name),
                                                                          sql.Identifier(col_name),
                                                                          sql.Literal(current_text),
                                                                          sql.Literal(id_number))
        # print(query)
        cur.execute(query)
        self.can_change = False
        self.db_conn.commit()
        cur.close()

    def accept_changing(self):
        self.can_change = True


def except_hook(cls, exception, traceback):
    sys.__excepthook__(cls, exception, traceback)


if __name__ == '__main__':
    app = QApplication(sys.argv)
    win = MainWin()
    win.show()
    sys.excepthook = except_hook
    sys.exit(app.exec())
