#!/usr/bin/python
import sys
from HistoryClient import *
from PyQt4.QtGui import *
from PyQt4.QtCore import *
from PyQt4.Qt import *


class MainWindow(QMainWindow):
    
    def __init__(self):
        # Init parent class for resizing etc...
        QMainWindow.__init__(self)
        
        # History Client
        self.client = HistoryClient()
        #self.requests = {}
        self.readResponseSubscriptions = []
        
        self.setWindowTitle('History Client')
        
        # Create Tab widget and fill with
        # pages
        self._build_tab_widget()
        
        page = self._build_requests_page()
        self.tabWidget.addTab(page, 'Requests')
       
        page = self._build_read_requests_page()
        self.tabWidget.addTab(page, 'Read')
        
        page = self._build_read_requests_page()
        self.tabWidget.addTab(page, 'DataBases')
       
       
        self.fetchRequests()


    def _build_tab_widget(self):
        
        self.tabWidget = QTabWidget()
        self.setCentralWidget(self.tabWidget)
        
    
    def _build_requests_page(self):
        
        addRequestBox = QHBoxLayout()
        
        self.lineEdit = QLineEdit("")
        addRequestBox.addWidget(self.lineEdit)

        self.addbutton = QPushButton("Add Request")
        self.addbutton.clicked.connect(self.addRequest)
        addRequestBox.addWidget(self.addbutton)
        
       
        requestsBox = QHBoxLayout()
        requestActionsBox = QVBoxLayout()
                
        self.listWidget = QListWidget()
        self.listWidget.itemClicked.connect(self._enableDeleteButton)
        self.listWidget.itemClicked.connect(self._enableReadButton)
        self.listWidget.doubleClicked.connect(self._showInfoDialog)
        requestsBox.addWidget(self.listWidget)
        
        self.readButton = QPushButton("Read Request")
        self.readButton.setEnabled(False)
        self.readButton.clicked.connect(self.readRequest)
        requestActionsBox.addWidget(self.readButton)
        
        self.deleteButton = QPushButton("Delete Request")
        self.deleteButton.setEnabled(False)
        self.deleteButton.clicked.connect(self.deleteRequest)
        requestActionsBox.addWidget(self.deleteButton)
        
        requestsBox.addLayout(requestActionsBox)


        layout = QVBoxLayout()
        layout.addLayout(addRequestBox)
        layout.addLayout(requestsBox)
        
        
        page = QWidget()
        page.setLayout(layout)
        return page
    
    
    def _build_read_requests_page(self):
        
        layout = QVBoxLayout()
        
        page = QWidget()
        page.setLayout(layout)
        return page
    
    
    def _build_dbs_page(self):
        
        layout = QVBoxLayout()
        
        page = QWidget()
        page.setLayout(layout)
        return page
    
    
    def _enableDeleteButton(self):
        self.deleteButton.setEnabled(True)
        
    def _enableReadButton(self):
        self.readButton.setEnabled(True)
        
    def _showInfoDialog(self):
        text = QLabel('ciao')
        vbox = QVBoxLayout()
        vbox.addWidget(text)
        
        dialog = QDialog(self)
        dialog.setLayout(vbox)
        dialog.show()
    
    
    def fetchRequests(self):
        requests = self.client.showHistoryRequests()
        
        for r in requests:
            lvi = QListWidgetItem(r['sparql'], self.listWidget)
            lvi.setData(Qt.UserRole, QVariant(r['uri']))

    
    def addRequest(self):
        sparql = self.lineEdit.text()
        request_uri = self.client.addHistoryRequest(sparql)
        
        lvi = QListWidgetItem(sparql, self.listWidget)
        lvi.setData(Qt.UserRole, QVariant(request_uri))
        print request_uri


    def readRequest(self):
        # Issue a history read on the currently selected item
        item = self.listWidget.item(self.listWidget.currentRow())
        request_uri  = item.data(Qt.UserRole).toPyObject()
        
        # History read. Automatically fetch sparql from the request
        self.client.readHistoryRequestData(request_uri, HistoryReadResponseHandler())
        
        # Disable others requests untill dont receive a response!
        self.readButton.setEnabled(False)
        

    def deleteRequest(self):
        # Remove the currently selected item from the listview.
        item = self.listWidget.takeItem(self.listWidget.currentRow())
        request_uri  = item.data(Qt.UserRole).toPyObject()
        
        self.client.deleteHistoryRequest(request_uri)
        
        # Check if the list is empty - if yes, disable the deletebutton.
        if self.listWidget.count() == 0:
            self.deleteButton.setEnabled(False)
            
    
    def closeEvent(self, *args, **kwargs):
        self.client.quit()
        #return QMainWindow.closeEvent(self, *args, **kwargs)


class HistoryReadResponseHandler():
    
    def handle(self, added, removed):
        # removed should be []
        # added should contain only 1 result with 1 variable
        # [ [['res', 'literal', 'your_xml_response']] ]
        
        response = parse_sparql(added[0][0][2])
        print response
        
        for i,result in enumerate(response):
            print '\n'
            for var in result:
                print str(i)+') '+var[0]+' = '+var[2]+'; '+var[1]

# Set up the main window
app = QApplication(sys.argv)
main = MainWindow()
main.show()

# Launches QT app main window and returns when it close
sys.exit(app.exec_())