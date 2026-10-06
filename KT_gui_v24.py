import sys
import json
import time
import os
import re
import random
import base64
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QPushButton, QTableWidget, QTableWidgetItem, QHeaderView,
    QTabBar, QStackedWidget, QGroupBox, QGridLayout, QMessageBox, QSplitter, QSlider,
    QLineEdit, QAbstractItemView, QCheckBox, QComboBox,QStyle, QHBoxLayout, QSpacerItem, QSizePolicy,QFrame
)
from PyQt6.QtCore import Qt, QTimer, QAbstractTableModel, QThread, pyqtSignal, QRect, QSize, QPropertyAnimation,pyqtProperty
from PyQt6.QtGui import QColor, QBrush, QFont, QPainter, QDoubleValidator,QShortcut,QKeySequence,QPalette,QColor,QPixmap,QIcon
from PyQt6.QtWidgets import QStyledItemDelegate, QWidget, QVBoxLayout, QHBoxLayout, QPushButton,QComboBox, QLineEdit, QListWidget, QLabel, QFrame, QTableWidget


####
color_theme = "#00B19D" #16A085
color_theme_hover = "#009482" #138871
color_theme_pressed = "#007365" #0E6655
color_success = "#51C285"
color_success_hover = "#3FA86D"
color_success_press = "#2F7E52"
color_warning = "#ED9B16"
color_fail = "#FF7B63"
color_fail_hover = "#DA6857"
color_fail_press = "#B4554B"
color_text = "#FFFFFF"
color_grey1 = "#90969E" #- 666666
color_grey2 = "#717B85" #- 555555
color_grey3 = "#1B2937" #- 444444 - 2a2a2a
color_grey4 = "#263544" #- gridline color
color_background = "#0A1929" #- 0A1929 0A1929

####

GroupIDtime = datetime.now().strftime("%Y-%m-%d_%H:%M:%S_")
 
class CurrencyTableWidgetItem(QTableWidgetItem):
    def __init__(self, value, display_format="auto"):
        """
        display_format:
            - "int"    → $ 123,456
            - "float"  → $ 123,456.78
            - "auto"   → float ise 2 ondalık, int ise tam sayı
        """
        self.value = value
 
        if display_format == "int":
            text = f"$ {int(value):,}"
        elif display_format == "float":
            text = f"$ {value:,.2f}"
        elif display_format == "auto":
            text = f"$ {value:,.2f}" if isinstance(value, float) else f"$ {int(value):,}"
        else:
            text = str(value)
 
        super().__init__(text)
        self.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
    def __lt__(self, other):
        if isinstance(other, CurrencyTableWidgetItem):
            return self.value < other.value
        return super().__lt__(other)
     
class HoverDelegate(QStyledItemDelegate):
    def paint(self, painter, option, index):
        if option.state & QStyle.StateFlag.State_MouseOver:
            painter.fillRect(option.rect, QColor("#2A3548"))  # hover rengi
        elif option.state & QStyle.StateFlag.State_Selected:
            painter.fillRect(option.rect, QColor("#00B19D"))  # seçili rengi (tek renk)
        QStyledItemDelegate.paint(self, painter, option, index)

class FilterWidget(QWidget):

    def __init__(self, table: QTableWidget, parent=None):
        super().__init__(parent)
        self.table = table
        self.active_filters = []
        self.toggle_btn = QPushButton("🔍 Filter")
        self.toggle_btn.setObjectName("FilterToggle")
        self.toggle_btn.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
        self.toggle_btn.setFixedHeight(32)
        self.toggle_btn.setFixedWidth(64)
        self.toggle_btn.setIconSize(QSize(14, 14))
        # f-string’de süslüleri KAÇIR: {{ }}
        self.toggle_btn.setStyleSheet(f"""
        QWidget QPushButton#FilterToggle {{
        background-color: {color_success};
        color: white;
        font-weight: 600;
        border: 1px solid {color_success};
        border-radius: 6px;
        padding: 4px 10px;
        min-width: 84px;
        min-height: 28px;
        }}
        QWidget QPushButton#FilterToggle:hover {{
        background-color: {color_success_hover};
        }}
        QWidget QPushButton#FilterToggle:pressed {{
        background-color: {color_success_press};
        }}
        """)
        # self.toggle_btn.setFixedWidth(70)
        self.toggle_btn.clicked.connect(self.toggle_visibility)
        # Frame that holds the filter UI
        self.frame = QFrame()
        self.frame.setFrameShape(QFrame.Shape.StyledPanel)
        self.frame.setVisible(False)
        # Column selector
        self.col_select = QComboBox()
        self.col_select.addItems(
            self.table.horizontalHeaderItem(i).text()
            for i in range(self.table.columnCount())
        )
        # Operator selector
        self.op_select = QComboBox()
        self.op_select.addItems(["contains","does not contain", "equals","does not equal", ">", "<"])
        # Value input
        self.val_input = QLineEdit()
        # Add filter button
        self.add_btn = QPushButton("Add Filter")
        self.add_btn.clicked.connect(self.add_filter)
        # Logic (AND / OR)
        self.logic_combo = QComboBox()
        self.logic_combo.addItems(["AND", "OR"])
        # Current filters list
        self.filter_list = QListWidget()
        # Apply / Clear buttons
        self.apply_btn = QPushButton("Apply")
        self.apply_btn.clicked.connect(self.apply_filters)
        self.clear_btn = QPushButton("Clear All")
        self.clear_btn.clicked.connect(self.clear_filters)
        # Layouts
        filter_row = QHBoxLayout()
        filter_row.addWidget(QLabel("Col:"))
        filter_row.addWidget(self.col_select)
        filter_row.addWidget(QLabel("Op:"))
        filter_row.addWidget(self.op_select)
        filter_row.addWidget(QLabel("Val:"))
        filter_row.addWidget(self.val_input)
        filter_row.addWidget(self.add_btn)
        logic_row = QHBoxLayout()
        logic_row.addWidget(QLabel("Logic:"))
        logic_row.addWidget(self.logic_combo)
        logic_row.addWidget(self.apply_btn)
        logic_row.addWidget(self.clear_btn)
        layout = QVBoxLayout()
        layout.addLayout(filter_row)
        layout.addWidget(QLabel("Current Filters:"))
        layout.addWidget(self.filter_list)
        layout.addLayout(logic_row)
        self.frame.setLayout(layout)

    # ----------------- METHODS ----------------- #

    def toggle_visibility(self):
        self.frame.setVisible(not self.frame.isVisible())
    def add_filter(self):
        col_index = self.col_select.currentIndex()
        col_name = self.col_select.currentText()
        operator = self.op_select.currentText()
        value = self.val_input.text().strip()
        if not value:
            return
        display = f"{col_name} {operator} {value}"
        self.filter_list.addItem(display)
        condition = self.create_filter_function(operator, value)
        self.active_filters.append((col_index, condition))
        self.val_input.clear()
    def apply_filters(self, keep_open=False):
        logic = self.logic_combo.currentText()
        for row in range(self.table.rowCount()):
            result = True if logic == "AND" else False
            for col_index, func in self.active_filters:
                item = self.table.item(row, col_index)
                match = func(item.text()) if item else False
                if logic == "AND":
                    result = result and match
                else:
                    result = result or match
            self.table.setRowHidden(row, not result)
        if not keep_open:
            self.frame.setVisible(False)
    def clear_filters(self):
        self.filter_list.clear()
        self.active_filters = []
        for row in range(self.table.rowCount()):
            self.table.setRowHidden(row, False)
    def create_filter_function(self, operator, value):
        """
        operator: seçilen karşılaştırma operatörü (">", "<", "equals", "contains")
        value: kullanıcı input değeri (string)
        """
        # Yardımcı: Timestamp parse
        def parse_timestamp(text: str):
            if not text:
                return None
            t = text.strip()
            for fmt in ["%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"]:
                try:
                    return datetime.strptime(t, fmt)
                except ValueError:
                    continue
            return None
        # Yardımcı: sayıyı normalize et (para işareti, %, boşluk, , . temizle)
        def normalize_number(text: str):
            if text is None:
                return None
            # $ - , % ve boşlukları kaldır
            cleaned = re.sub(r"[^\d\-\.\,]", "", text)
            # virgülü nokta ile değiştir
            cleaned = cleaned.replace(",", "")
            try:
                return float(cleaned)
            except Exception:
                return None
        # Kullanıcı input'unu datetime olarak dene
        v_dt = parse_timestamp(value)
        # Kullanıcı input'unu sayı olarak dene
        v_num = None
        if v_dt is None:
            v_num = normalize_number(value)
        def condition(text: str):
            if text is None:
                return False
            s = text.strip()
            # 1) Timestamp karşılaştırması
            t_dt = parse_timestamp(s)
            if t_dt and v_dt:
                if operator == ">":
                    return t_dt > v_dt
                if operator == "<":
                    return t_dt < v_dt
                if operator == "equals":
                    return t_dt == v_dt
                if operator=="does not equal":
                    return t_dt != v_dt
            # 2) Sayısal karşılaştırma
            t_num = normalize_number(s)
            if v_num is not None and t_num is not None:
                if operator == ">":
                    return t_num > v_num
                if operator == "<":
                    return t_num < v_num
                if operator == "equals":
                    return t_num == v_num
                if operator == "does not equal":
                    return t_num != v_num
            # 3) Metin karşılaştırma
            if operator == "equals":
                return s == value.strip()
            if operator == "does not equal":
                return s != value.strip()
            if operator == "contains":
                return value.lower() in s.lower()
            if operator=="does not contain":
                return value.lower() not in s.lower()
            return False
        return condition
    # def create_filter_function(self, operator, value):
    #     def normalize_number(text):
    #         # "$ 47,924" -> "47924"
    #         return text.replace("$", "").replace(",", "").strip()
    #     def condition(text):
    #         try:
    #             if operator == "equals":
    #                 return text == value
    #             elif operator == "contains":
    #                 return value in text
    #             elif operator == ">":
    #                 return float(normalize_number(text)) > float(value)
    #             elif operator == "<":
    #                 return float(normalize_number(text)) < float(value)
    #         except Exception:
    #             return False
    #     return condition
 
class PaddingDelegate(QStyledItemDelegate):
    def __init__(self, padding=20, parent=None):
        super().__init__(parent)
        self.padding = padding
    def paint(self, painter, option, index):
        try:
            padded_rect = QRect(
                option.rect.left() + self.padding,
                option.rect.top() + self.padding,
                option.rect.width() - 2 * self.padding,
                option.rect.height() - 2 * self.padding
            )
            option.rect = padded_rect
            super().paint(painter, option, index)
        except Exception as e:
            print(f"Delegate paint error: {e}")

class NumberTableWidgetItem(QTableWidgetItem):
    """Custom table item for numeric values that sorts correctly and aligns text to the right"""
    def __init__(self, text, numeric_value):
        super().__init__(text)
        self.numeric_value = numeric_value
        self.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
 
    def __lt__(self, other):
        if isinstance(other, NumberTableWidgetItem):
            return self.numeric_value < other.numeric_value
        return super().__lt__(other)

class NonEditableDelegate(QStyledItemDelegate):
    """Delegate that makes table items non-editable"""
    def createEditor(self, parent, option, index):
        return None  # Return None to prevent editing

class KafkaWorker(QThread):
    """Worker thread for Kafka operations"""
    data_updated = pyqtSignal(str, str)  # topic, data (string format)
    error_occurred = pyqtSignal(str)
    
    def __init__(self, bootstrap_servers=['localhost:9092']):
        super().__init__()
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.consumers = {}
        self.running = True
        
    def run(self):
        try:
            # Initialize producer with connection timeout
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: v.encode('utf-8') if isinstance(v, str) else str(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
            )
            
            # Test connection
            self.producer.metrics()
            print("Successfully connected to Kafka broker")
            
            # Initialize consumers for each topic - optimized for real-time consumption
            topics = ['PanelPosition_Sim2', 'PanelExecutionResult_Sim2', 'PanelStrategy_Sim2', 'PanelRiskMonitor_Sim2', 
                      'PanelProvider_Sim2', 'PanelCustomerData_Sim2','PanelCustomerData_Sim2_InternalTrade','PanelClosePosition_Sim2',
                     'PanelPositionPNLSummary_Sim2', 'PanelCustomerFlowSummary_Sim2', 'PanelExecutionSummary_Sim2', 'PanelCurrencyFlowSummary_Sim2']
            for topic in topics:
                if topic in []:
                    auto_offset_reset_variable = 'earliest'
                else:
                    auto_offset_reset_variable = 'latest'

                consumer = KafkaConsumer(
                    topic,
                    bootstrap_servers=self.bootstrap_servers,
                    value_deserializer=lambda m: m.decode('utf-8'),  # Raw string deserializer like table viewer
                    auto_offset_reset=auto_offset_reset_variable,
                    enable_auto_commit=True,
                    group_id=str(GroupIDtime)+f'KT_gui_{topic}',
                )
                self.consumers[topic] = consumer
                
            # Start consuming messages with ultra-low latency
            while self.running:
                try:
                    # Process all consumers in parallel for maximum throughput
                    all_messages = {}
                    for topic, consumer in self.consumers.items():
                        if not self.running:
                            break
                        # Ultra-fast polling for real-time consumption
                        messages = consumer.poll(timeout_ms=10, max_records=100)  # 10ms timeout for maximum speed
                        if messages:
                            all_messages[topic] = messages
                    
                    # Process all received messages immediately
                    for topic, messages in all_messages.items():
                        for tp, records in messages.items():
                            for record in records:
                                if not self.running:
                                    break
                                from datetime import datetime
                                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                #print(f"[{current_time}] Kafka worker received {topic}: {record.value}")
                                self.data_updated.emit(topic, record.value)
                except Exception as e:
                    if self.running:  # Only log if we're not shutting down
                        print(f"Consumer error: {e}")
                    break
                
        except KafkaError as e:
            error_msg = f"Kafka error: {str(e)}"
            if "no brokers found" in str(e).lower():
                error_msg = "Kafka broker not found. Please ensure Kafka is running on localhost:9093"
            self.error_occurred.emit(error_msg)
        except Exception as e:
            self.error_occurred.emit(f"Worker error: {str(e)}")
    
    def stop(self):
        """Stop all Kafka connections gracefully"""
        print("Stopping Kafka worker...")
        self.running = False
        
        # Give the consumer loop time to exit
        time.sleep(0.1)
        
        # Close producer gracefully
        if self.producer and not self.producer._closed:
            try:
                self.producer.flush(timeout=5)  # Flush any pending messages
                self.producer.close(timeout=5)
                print("Producer closed")
            except Exception as e:
                print(f"Error closing producer: {e}")
        
        # Close all consumers gracefully
        for topic, consumer in self.consumers.items():
            try:
                if consumer and not consumer._closed:
                    consumer.close()
                    print(f"Consumer for {topic} closed")
            except Exception as e:
                print(f"Error closing consumer for {topic}: {e}")
        
        print("All Kafka connections closed")
    
    def check_kafka_connection(self):
        """Check if Kafka is accessible"""
        try:
            import socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex(('localhost', 9093))
            sock.close()
            return result == 0
        except Exception:
            return False
    
    def send_message(self, topic, key, data):
        """Send message to Kafka topic"""
        try:
            if self.producer and not self.producer._closed:
                future = self.producer.send(topic, key=key, value=data)
                future.get(timeout=10)
                return True
        except Exception as e:
            self.error_occurred.emit(f"Send error: {str(e)}")
            return False

class Switch(QWidget):
   def __init__(self, parent=None):
       super().__init__(parent)
       self.setFixedSize(48, 20)
       self._checked = False
       self._handle_position = 2
       self.animation = QPropertyAnimation(self, b"handle_position", self)
       self.animation.setDuration(150)
   def mousePressEvent(self, event):
       self._checked = not self._checked
       self.animate()
       self.update()
       print("Switch AÇIK" if self._checked else "Switch KAPALI")
   def animate(self):
       end = self.width() - 20 if self._checked else 2
       self.animation.stop()
       self.animation.setEndValue(end)
       self.animation.start()
   def paintEvent(self, event):
       p = QPainter(self)
       p.setRenderHint(QPainter.RenderHint.Antialiasing)
       # Arka plan
       bg_color = QColor("#00B19D" ) if self._checked else QColor("#717B85")
       p.setBrush(QBrush(bg_color))
       p.setPen(Qt.PenStyle.NoPen)
       p.drawRoundedRect(self.rect(), 10, 10)
       # Handle (içte kayan yuvarlak)
       p.setBrush(QBrush(QColor("#F0F5FA") if self._checked else QColor("#E0E0E0")))
       p.drawEllipse(QRect(int(self._handle_position), 1, 18, 18))
   def sizeHint(self):
       return QSize(48, 20)
   def get_handle_position(self):
       return self._handle_position
   def set_handle_position(self, pos):
       self._handle_position = pos
       self.update()
   handle_position = pyqtProperty(float, fget=get_handle_position, fset=set_handle_position)

class TradingApp(QMainWindow):

    def __init__(self):
        super().__init__()
        self.setWindowTitle("KT Trading Application")
        self.setGeometry(100, 100, 1400, 900)
        
        # Initialize data structures
        self.position_data = {}
        self.strategy_data = {}
        self.provider_data = {}
        self.execution_data_list = []
        self.risk_data = {}
        self.position_pnl_data = {}
        self.customer_flow_data = {}
        self.execution_summary_data = {}
        self.currency_flow_data = {}
        self.customer_transaction_data_list = []
        
        # Flag to track when waiting for Kafka confirmation
        self.waiting_for_strategy_kafka_confirmation = False
        self.waiting_for_provider_kafka_confirmation = False
        self.pending_strategy_confirmations = set()  # Track pending strategy confirmations
        self.pending_provider_confirmations = set()
        # Initialize Kafka worker
        self.kafka_worker = KafkaWorker()
        self.kafka_worker.data_updated.connect(self.handle_kafka_update)
        self.kafka_worker.error_occurred.connect(self.handle_kafka_error)
        
        # Check Kafka connection before starting
        if not self.kafka_worker.check_kafka_connection():
            QMessageBox.warning(self, "Kafka Connection", 
                              "Kafka is not running on localhost:9093.\n\n"
                              "To start Kafka:\n"
                              "1. Install Docker Desktop\n"
                              "2. Run: docker-compose up -d\n\n"
                              "The application will continue in demo mode.")
        else:
            print("Kafka connection verified")
            
        self.kafka_worker.start()
        
        # Initialize data storage
        self.execution_data = {}
        self.customer_transaction_data = {}
        self.strategy_file = "strategy_data.json"
        self.provider_file = {}
        self.strategy_data_initialized = False  # Flag to track if strategy data has been initialized from Kafka
        self.provider_data_initialized = False  # Flag to track if provider data has been initialized from Kafka
        
        # Overview data storage
        self.position_pnl_data = {}
        self.customer_flow_data = {}
        self.execution_summary_data = {}
        self.currency_flow_data = {}
        
        # Strategy editing state
        self.editing_row = None  # Currently editing row
        self.original_values = {}  # Store original values before editing
        
        # Clear all data structures to ensure clean state
        self.clear_all_data_structures()

        #ªfull screen
        self.showMaximized()
        
        # Load strategy data after clearing to ensure it's available when creating tables
        ##        self.load_strategy_data()
                
        self.apply_dark_theme()

        # Central widget and main layout
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        main_layout.setContentsMargins(10, 10, 10, 10)

        # --- Top bar: TabBar + Controls ---
        topbar_layout = QHBoxLayout()
        topbar_layout.setSpacing(10)
        topbar_layout.setContentsMargins(0, 0, 0, 0)

        # TabBar (not TabWidget!)
        self.tabbar = QTabBar()
        self.tab_names = [
            "Trade", "Transaction List", "Providers - Manual Trading", "Overview", "Report", "Analytics"
        ]
        for name in self.tab_names:
            self.tabbar.addTab(name)
        self.tabbar.setCurrentIndex(0)
        self.tabbar.currentChanged.connect(self.switch_tab)
        self.tabbar.setStyleSheet(f"""
            QTabBar::tab {{
                background-color: #1B2937; color: #ffffff; padding: 12px 12px; margin-right: 2px;
                border: 1px solid #263544; border-bottom: none; border-top-left-radius: 4px; border-top-right-radius: 4px;
                min-width: 150px;
            }}
            QTabBar::tab:selected {{ background-color: {color_theme}; color: #ffffff; font-weight: bold; }}
            QTabBar::tab:hover {{ background-color: {color_theme_hover}; }}
            QTabBar::tab:pressed {{ background-color: {color_theme_pressed}; }}
        """)

        topbar_layout.addWidget(self.tabbar, 0)

        # Controls group (account buttons, metrics, start/stop)
        controls_group = QWidget()
        controls_layout = QHBoxLayout(controls_group)
        controls_layout.setContentsMargins(0, 0, 0, 0)
        controls_layout.setSpacing(10)

        # Account buttons
        account_box = QGroupBox("Accounts")
        account_layout = QGridLayout(account_box)
        accounts = [("Kuwait", 0, 0), ("Bahrain", 0, 1), ("Turkey", 1, 0), ("ALL", 1, 1)]
        self.account_buttons = {}
        for account, row, col in accounts:
            btn = QPushButton(account)
            btn.setCheckable(True)
            btn.setFixedSize(60, 20)
            btn.setStyleSheet(f"""
                QPushButton {{
                    background-color: #1B2937; color: #ffffff; border: 1px solid #1B2937;
                    border-radius: 3px; font-size: 11px; font-weight: bold;
                    padding: 0px;
                }}
                QPushButton:checked {{ background-color: {color_theme}; color: #ffffff; }}
                QPushButton:hover {{ background-color: {color_theme_hover}; }}
                QPushButton:pressed {{ background-color: {color_theme_pressed}; }}
            """)
            if account in ["Kuwait"]:
                btn.setChecked(True)
            account_layout.addWidget(btn, row, col)
            self.account_buttons[account] = btn
        controls_layout.addWidget(account_box)

        # Metrics
        metrics_box = QGroupBox("Metrics")
        metrics_layout = QVBoxLayout(metrics_box)
        metrics_layout.setContentsMargins(5, 5, 5, 5)
        metrics_layout.setSpacing(2)
        pnl_layout = QHBoxLayout()
        pnl_label = QLabel("Cumulative PnL:")
        pnl_label.setStyleSheet("color: white; font-weight: bold; font-size: 10px;")
        pnl_layout.addWidget(pnl_label)
        self.pnl_value = QLabel("$ 165,039.00")
        self.pnl_value.setStyleSheet(f"color: #51C285 ; font-weight: bold; font-size: 10px;")
        pnl_layout.addWidget(self.pnl_value)
        metrics_layout.addLayout(pnl_layout)
        pos_layout = QHBoxLayout()
        pos_label = QLabel("Cumulative Position:")
        pos_label.setStyleSheet("color: white; font-weight: bold; font-size: 10px;")
        pos_layout.addWidget(pos_label)
        self.pos_value = QLabel("$ 4,467,059.00")
        self.pos_value.setStyleSheet("color: white; font-weight: bold; font-size: 11px;")
        pos_layout.addWidget(self.pos_value)
        metrics_layout.addLayout(pos_layout)
        controls_layout.addWidget(metrics_box)



        topbar_layout.addWidget(controls_group, 0)
        topbar_layout.addStretch(2)
        main_layout.addLayout(topbar_layout)

        # --- Main content area: QStackedWidget for tab content ---
        self.stacked = QStackedWidget()
        self.pages = []
        self.pages.append(self.create_trade_tab())
        self.pages.append(self.create_transaction_tab()) #"Transaction List Page")
        self.pages.append(self.create_provider_tab()) #"Providers & Manual Trading Page")
        self.pages.append(self.create_overview_tab())
        self.pages.append(self.create_placeholder_tab("Report Page"))
        self.pages.append(self.create_placeholder_tab("Analytics Page"))
        for page in self.pages:
            self.stacked.addWidget(page)
        main_layout.addWidget(self.stacked)

        # Initialize execution data and manual trade list
        self.execution_data_list = []
        self.manual_trade_list = []        
        self.customer_transaction_data_list = []        
        self.internal_transaction_data_list = []            
        

        # Populate tables immediately - they will update as Kafka data arrives
        self.delayed_table_population()
        
        # Set up controlled GUI updates at fixed intervals
        self.gui_update_timer = QTimer()
        self.gui_update_timer.timeout.connect(self.update_all_tables_from_data)
        self.gui_update_timer.start(1000)  # Update GUI every 1 second
        
        # Resize all table columns immediately
        self.resize_all_table_columns()

    def resize_all_table_columns(self):
        """Resize all table columns immediately"""
        # Only resize tables that exist
        tables = []
        if hasattr(self, 'position_table') and self.position_table:
            tables.append(self.position_table)
        if hasattr(self, 'strategy_table') and self.strategy_table:
            tables.append(self.strategy_table)
        if hasattr(self, 'execution_table') and self.execution_table:
            tables.append(self.execution_table)
        if hasattr(self, 'risk_table') and self.risk_table:
            tables.append(self.risk_table)
        if hasattr(self, 'position_pnl_table') and self.position_pnl_table:
            tables.append(self.position_pnl_table)
        if hasattr(self, 'customer_flow_table') and self.customer_flow_table:
            tables.append(self.customer_flow_table)
        if hasattr(self, 'execution_summary_table') and self.execution_summary_table:
            tables.append(self.execution_summary_table)
        if hasattr(self, 'currency_flow_table') and self.currency_flow_table:
            tables.append(self.currency_flow_table)
        if hasattr(self, 'customer_transaction_table') and self.customer_transaction_table:
            tables.append(self.customer_transaction_table)
        if hasattr(self, 'internal_transaction_table') and self.internal_transaction_table:
            tables.append(self.internal_transaction_table)
        if hasattr(self, 'manual_trade_table') and self.manual_trade_table:
            tables.append(self.manual_trade_table)
        if hasattr(self, 'provider_table') and self.provider_table:
            tables.append(self.provider_table)

        for table in tables:
            if table:
                header = table.horizontalHeader()
                
                # Resize each column to fit its content
                for i in range(table.columnCount()):
                    # Get the header text
                    header_text = table.horizontalHeaderItem(i).text() if table.horizontalHeaderItem(i) else ""
                    
                    # Calculate minimum width needed for header
                    header_width = len(header_text) * 8  # Approximate character width
                    
                    # Set minimum width to ensure header is visible
                    min_width = max(140, header_width + 20)  # Add padding
                    table.setColumnWidth(i, min_width)
                
                # Ensure the table can show all columns
                table.resizeColumnsToContents()
                
                # Set a reasonable maximum width to prevent columns from being too wide
                for i in range(table.columnCount()):
                    if table.columnWidth(i) > 200:
                        table.setColumnWidth(i, 200)

    def apply_dark_theme(self):
        self.setStyleSheet("""
            QMainWindow { background-color: #0A1929; color: #ffffff; }
            QWidget { background-color: #0A1929; color: #ffffff; }
            QTableWidget {
                background-color: #0A1929; alternate-background-color: #1B2937;
                gridline-color: #263544; color: #ffffff; border: 1px solid #263544;
            }     
            QTableWidget::item:selected { background-color: #00B19D; color: #000000; }
            QLabel::item:selected { background-color: #00B19D; color: #000000; }
            QTableWidget::item:hover { background-color: #00B19D; color: #000000; }
            QHeaderView::section {
                background-color: #1B2937; color: #ffffff; padding: 8px;
                border: 0.5px solid #263544; font-weight: bold;
            }
            QPushButton {
                background-color: #1B2937; color: #ffffff; border: 1px solid #90969E;
                padding: 8px 16px; border-radius: 4px; font-weight: bold;
            }
            QPushButton:hover { background-color: #717B85; }
            QPushButton:pressed { background-color: #ff9900; color: #000000; }
            QLabel {color: #ffffff; }
            QSplitter::handle {
                background-color: #263544;
                border: 1px solid #1B2937;
            }
            QSplitter::handle:hover {
                background-color: #263544;
            }
        """)
        label = QLabel("Selectable text")
        label.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        palette = self.palette()
        palette.setColor(QPalette.ColorRole.Highlight, QColor("yellow"))
        palette.setColor(QPalette.ColorRole.HighlightedText, QColor("red"))
        self.setPalette(palette)

#Create Tabs

    def switch_tab(self, idx):
        self.stacked.setCurrentIndex(idx)

    def create_trade_tab(self):
        trade_widget = QWidget()
        trade_layout = QVBoxLayout(trade_widget)
        trade_layout.setContentsMargins(5, 5, 5, 5)

        # Create vertical splitter for top and bottom sections
        vertical_splitter = QSplitter(Qt.Orientation.Vertical)
        
        # Top section: Position/Price and Strategy tables
        top_splitter = QSplitter(Qt.Orientation.Horizontal)
        position_widget = self.create_position_table()
        strategy_widget = self.create_strategy_table()
        top_splitter.addWidget(position_widget)
        top_splitter.addWidget(strategy_widget)
        top_splitter.setSizes([600, 600])  # Initial sizes
        
        # Bottom section: Execution Results and Risk Monitor tables
        bottom_splitter = QSplitter(Qt.Orientation.Horizontal)
        execution_widget = self.create_execution_table()
        risk_widget = self.create_risk_table()
        bottom_splitter.addWidget(execution_widget)
        bottom_splitter.addWidget(risk_widget)
        bottom_splitter.setSizes([600, 600])  # Initial sizes
        
        # Add both sections to vertical splitter
        vertical_splitter.addWidget(top_splitter)
        vertical_splitter.addWidget(bottom_splitter)
        vertical_splitter.setSizes([500, 500])  # Initial sizes
        
        trade_layout.addWidget(vertical_splitter)
        
        return trade_widget

    def create_placeholder_tab(self, text):
        widget = QWidget()
        layout = QVBoxLayout(widget)
        placeholder = QLabel(text)
        placeholder.setAlignment(Qt.AlignmentFlag.AlignCenter)
        placeholder.setStyleSheet("color: #888888; font-size: 18px;")
        layout.addWidget(placeholder)
        return widget

    def create_provider_tab(self):
        """Create overview page with 3 tables in custom 2-column layout"""
        overview_widget = QWidget()
        overview_layout = QVBoxLayout(overview_widget)
        overview_layout.setContentsMargins(5, 5, 5, 5)
        # Header
        header_widget = QWidget()
        header_layout = QHBoxLayout(header_widget)
        header_layout.setContentsMargins(8, 8, 8, 8)
        header_layout.addStretch()

        # header_layout.addWidget(self.overview_period_combo)
        overview_layout.addWidget(header_widget)
        # Ana yatay splitter (Sol - Sağ)
        main_splitter = QSplitter(Qt.Orientation.Horizontal)
        # Sol taraf: dikey olarak 2 tablo
        left_splitter = QSplitter(Qt.Orientation.Vertical)
        manual_trade_input_widget = self.create_manual_trade_input_table()  # 1. alt ekran
        execution_summary_widget = self.create_manual_trade_table()  # 2. alt ekran
        left_splitter.addWidget(manual_trade_input_widget)
        left_splitter.addWidget(execution_summary_widget)
        left_splitter.setSizes([400, 400])
        # Sağ taraf: tek tablo
        provider_widget = self.create_provider_table()  # 3. alt ekran
        main_splitter.addWidget(left_splitter)
        main_splitter.addWidget(provider_widget)
        main_splitter.setSizes([800, 600])
        overview_layout.addWidget(main_splitter, stretch=1)
        return overview_widget

    def create_transaction_tab(self):

        trade_widget = QWidget()
        trade_layout = QVBoxLayout(trade_widget)
        trade_layout.setContentsMargins(5, 5, 5, 5)

        top_splitter = QSplitter(Qt.Orientation.Horizontal)
        customer_transaction_widget = self.create_customer_transaction_table()
        internal_transaction_widget = self.create_internal_transaction_table()
        top_splitter.addWidget(customer_transaction_widget)
        top_splitter.addWidget(internal_transaction_widget)
        top_splitter.setSizes([600, 600])  # Initial sizes
        
        trade_layout.addWidget(top_splitter)
        
        return trade_widget

    def create_overview_tab(self):
        """Create overview page with 4 tables in 2x2 splitter layout"""
        overview_widget = QWidget()
        overview_layout = QVBoxLayout(overview_widget)
        overview_layout.setContentsMargins(5, 5, 5, 5)

        # Create header with combobox in top right corner
        header_widget = QWidget()
        header_layout = QHBoxLayout(header_widget)
        header_layout.setContentsMargins(8, 8, 8, 8)
        
        # Add stretch to push combobox to the right
        header_layout.addStretch()
        
        # Create combobox for time period selection
        period_label = QLabel("Time Period:")
        period_label.setStyleSheet("color: white;font-family: 'Roboto Mono';font-weight:bold; font-size: 12px; margin-right: 5px;")
        header_layout.addWidget(period_label)
        
        self.overview_period_combo = QComboBox()
        self.overview_period_combo.addItems(['Today', 'Week-to-date', 'Month-to-date', 'Quarter-to-date', 'Year-to-date', 'Custom'])
        self.overview_period_combo.setCurrentText('Month-to-date')  # Default selection
        self.overview_period_combo.setFixedWidth(150)
        self.overview_period_combo.setStyleSheet("""
        QComboBox {
            
            color: #FFFFFF;
            border: 1px solid #3C3C3C;
            border-radius: 5px;
            padding: 4px 8px;
            font-size: 12px;
            font-family: 'Roboto Mono';
            min-height: 24px;
        }
        QComboBox:hover {
            border: 1px solid #FF9900;
        }
        QComboBox:focus {
            border: 1px solid #FF9900;
        }
        QComboBox::drop-down {
            subcontrol-origin: padding;
            subcontrol-position: top right;
            width: 24px;
            border-left: 1px solid #3C3C3C;
            
        }
        QComboBox::down-arrow {
            image:url(C:/Users/tpmenes/Desktop/Python/GUI/KT/down.png);
            width: 12px;
            height: 12px;
        }
        QComboBox QAbstractItemView {
            color: #FFFFFF;
            
            selection-color: #000000;
            border: 1px solid #3C3C3C;
            font-family: 'Roboto Mono';
            font-size: 12px;
        }
            QComboBox QAbstractItemView::item {
            
            padding:5px;
        }
            """)
        # selection-background-color: #FF9900;
        header_layout.addWidget(self.overview_period_combo)
        
        overview_layout.addWidget(header_widget)

        # Create vertical splitter for top and bottom sections
        vertical_splitter = QSplitter(Qt.Orientation.Vertical)
        
        # Top section: Position & PNL and Position Flow Summary tables
        top_splitter = QSplitter(Qt.Orientation.Horizontal)
        position_pnl_widget = self.create_position_pnl_table()
        customer_flow_widget = self.create_customer_flow_table()
        top_splitter.addWidget(position_pnl_widget)
        top_splitter.addWidget(customer_flow_widget)
        top_splitter.setSizes([600, 600])  # Initial sizes
        
        # Bottom section: Execution Summary and Currency Flow Summary tables
        bottom_splitter = QSplitter(Qt.Orientation.Horizontal)
        execution_summary_widget = self.create_execution_summary_table()
        currency_flow_widget = self.create_currency_flow_table()
        bottom_splitter.addWidget(execution_summary_widget)
        bottom_splitter.addWidget(currency_flow_widget)
        bottom_splitter.setSizes([600, 600])  # Initial sizes
        
        # Add both sections to vertical splitter
        vertical_splitter.addWidget(top_splitter)
        vertical_splitter.addWidget(bottom_splitter)
        vertical_splitter.setSizes([500, 500])  # Initial sizes
        
        overview_layout.addWidget(vertical_splitter)
        
        return overview_widget
    
#Create Tables

    def create_position_pnl_table(self):
        """Create Position & PNL table"""
        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.setContentsMargins(0, 0, 0, 0)
        
        # Header with controls
        header_widget = QWidget()
        header_layout = QHBoxLayout(header_widget)
        header_layout.setContentsMargins(8, 8, 8, 8)
        
        # Header label
        header = QLabel("Position & PNL")
        header.setStyleSheet("color: white; font-weight: bold; font-size: 14px;")
        header_layout.addWidget(header)
        
        # Search input
        search_label = QLabel("Search:")
        search_label.setStyleSheet("color: white; font-size: 12px; margin-right: 5px;")
        header_layout.addWidget(search_label)
        
        self.position_pnl_search = QLineEdit()
        self.position_pnl_search.setPlaceholderText("Type to search...")
        self.position_pnl_search.setFixedWidth(200)
        self.position_pnl_search.setStyleSheet("""
            QLineEdit {
                background-color: #1B2937; color: white; border: 1px solid #263544;
                padding: 4px 8px; border-radius: 3px; font-size: 11px;
            }
            QLineEdit:focus { border: 1px solid #ff9900; }
        """)
        self.position_pnl_search.textChanged.connect(
        lambda text: self.filter_table_by_text(self.position_pnl_table, text))
        header_layout.addWidget(self.position_pnl_search)
        
        header_layout.addStretch()
        
        layout.addWidget(header_widget)
        
        # Table
        self.position_pnl_table = QTableWidget()
        columns = ["Currency", "Exposure", "USD Equivalent", "Avg Cost Rate", "Unrealized PNL", "Spread PNL", "Matching PNL", "Position PNL", "Total PNL"]
        self.position_pnl_table.setColumnCount(len(columns))
        self.position_pnl_table.setHorizontalHeaderLabels(columns)
        self.setup_table_properties(self.position_pnl_table)
        self.position_pnl_table.setItemDelegate(PaddingDelegate(padding=6, parent=self.position_pnl_table))
        layout.addWidget(self.position_pnl_table)
        return widget

    def create_manual_trade_input_table(self):
        """Create Manual Trade input table (Symbol, Side, Price, Venue + Add)"""
        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.setContentsMargins(0, 0, 0, 0)
        # Header
        header_widget = QWidget()
        header_layout = QHBoxLayout(header_widget)
        header_layout.setContentsMargins(8, 8, 8, 8)
        header = QLabel("Manual Trade Input")
        header.setStyleSheet("color: white; font-weight: bold; font-size: 14px;")
        header_layout.addWidget(header)
        header_layout.addStretch()
        layout.addWidget(header_widget)
        # Table: params top-to-bottom, inputs on the right
        self.manual_trade_table = QTableWidget(8, 2)
        self.manual_trade_table.setHorizontalHeaderLabels(["Parameter", "Value"])
        self.manual_trade_table.verticalHeader().setVisible(False)
        self.manual_trade_table.horizontalHeader().setStretchLastSection(True)
        self.manual_trade_table.setShowGrid(True)
        # (Opsiyonel) kendi ortak tablo ayarlarınız varsa kullanın
        if hasattr(self, "setup_table_properties"):
            self.setup_table_properties(self.manual_trade_table)
        self.manual_trade_table.setSortingEnabled(False)                      # <<< sıralamayı kapattık
        self.manual_trade_table.horizontalHeader().setSortIndicatorShown(False)  # <<< sıralama oku görünmesin
        self.manual_trade_table.horizontalHeader().setSectionsClickable(False)   # <<
        # Left column labels
        params = ["Symbol", "Side", "Price", "Amount", "Venue", "CounterParty", "Trade/TicketID", "User"]
        for r, name in enumerate(params):
            item = QTableWidgetItem(name)
            item.setFlags(Qt.ItemFlag.ItemIsEnabled)  # sadece görüntü
            self.manual_trade_table.setItem(r, 0, item)
        # Right column widgets
        # Symbol
        symbol_combo = QComboBox()
        symbol_combo.addItems(['USD/TRY','EUR/USD', 'XAU/USD','XAG/USD','XPT/USD','GBP/USD','USD/CHF','USD/SAR','USD/KWD','USD/JPY', 'USD/CAD', 'USD/SEK','USD/NOK','AUD/USD','NZD/USD', 'USD/AED', 'USD/BHD','USD/QAR', 'USD/OMR' ])
        symbol_combo.setCurrentIndex(-1)
        # Side
        side_combo = QComboBox()
        side_combo.addItems(["Buy", "Sell"])
        side_combo.setCurrentIndex(-1)
        # Price
        price_edit = QLineEdit()
        # price_edit.setPlaceholderText("e.g. 1.0850")
        price_edit.setValidator(QDoubleValidator(0.0, 1e9, 6))  # 6 ondalık
        # Amount
        amount_edit = QLineEdit()
        # amount_edit.setPlaceholderText("e.g. 1.0850")
        amount_edit.setValidator(QDoubleValidator(0.0, 1e9, 6))  # 6 ondalık
        # Venue
        venue_combo = QComboBox()
        venue_combo.setEditable(True)          # serbest girişe izin ver
        venue_combo.addItems(["KFH", "Integral", "Tradair", "360T"])  # örnek; kendi listenizi koyun
        venue_combo.setCurrentIndex(-1)
        # CounterParty
        counterparty_edit = QLineEdit()
        counterparty_edit.setPlaceholderText("e.g. JPM, GS")
        counterparty_edit.setMaxLength(20)
        # Trade/TicketID
        tradeid_edit = QLineEdit()
        tradeid_edit.setPlaceholderText("")
        tradeid_edit.setMaxLength(20)
        # User
        user_edit = QLineEdit()
        user_edit.setPlaceholderText("")
        user_edit.setMaxLength(20)
    
        # Görünümler (sizin koyu tema stilinize uyumlu)
        line_edit_css = """
            QLineEdit { background: transparent; color: white; 
                        padding: 4px 8px; border-radius: 1px; font-size: 11px; }
                    """
        combo_css = """
            QComboBox { background: transparent; color: white; 
                        padding: 4px 8px; font-size: 11px; }
                        QComboBox QAbstractItemView { background-color: #1B2937; color: white; }
        """

        # line_edit_css = """
        # QLineEdit { background: transparent; color: white; border: 1px solid #263544;
        #             padding: 4px 8px; border-radius: 3px; }
        # QLineEdit:focus { border: 1px solid #ff9900; }
        # """
        # combo_css = """
        # QComboBox { background: transparent; color: white; border: 1px solid #263544;
        #             padding: 4px 8px; border-radius: 3px; }
        # QComboBox:focus { border: 1px solid #ff9900; }
        # QComboBox QAbstractItemView { background-color: #1B2937; color: white; }  /* açılır liste */
        # """

        symbol_combo.setStyleSheet(combo_css)
        side_combo.setStyleSheet(combo_css)
        price_edit.setStyleSheet(line_edit_css)
        amount_edit.setStyleSheet(line_edit_css)
        venue_combo.setStyleSheet(combo_css)
        counterparty_edit.setStyleSheet(line_edit_css)
        tradeid_edit.setStyleSheet(line_edit_css)
        user_edit.setStyleSheet(line_edit_css)

        self.manual_trade_table.setCellWidget(0, 1, symbol_combo)
        self.manual_trade_table.setCellWidget(1, 1, side_combo)
        self.manual_trade_table.setCellWidget(2, 1, price_edit)
        self.manual_trade_table.setCellWidget(3, 1, amount_edit)
        self.manual_trade_table.setCellWidget(4, 1, venue_combo)
        self.manual_trade_table.setCellWidget(5, 1, counterparty_edit)
        self.manual_trade_table.setCellWidget(6, 1, tradeid_edit)
        self.manual_trade_table.setCellWidget(7, 1, user_edit)

        # Widget referanslarını saklayalım
        self.manual_inputs = {
            "symbol": symbol_combo,
            "side": side_combo,
            "price": price_edit,
            "amount": amount_edit,
            "venue": venue_combo,
            "counterparty": counterparty_edit,
            "tradeid": tradeid_edit,
            "user": user_edit,
        }
        self.manual_trade_table.setAlternatingRowColors(True)
        layout.addWidget(self.manual_trade_table)
        # Add button
        btn_row = QWidget()
        btn_layout = QHBoxLayout(btn_row)
        btn_layout.setContentsMargins(8, 8, 8, 8)
        btn_layout.addStretch()
        add_btn = QPushButton("Add")
        add_btn.setFixedHeight(28)
        add_btn.setFixedWidth(80)
        add_btn.setStyleSheet("QPushButton { background-color: #00B19D; color: white; border: none; padding: 4px 12px; border-radius: 3px; }"
                                "QPushButton:hover { background-color: #009482; }"
                                "QPushButton:pressed { background-color: #007365; }")
        add_btn.clicked.connect(self.on_add_manual_trade)
        btn_layout.addWidget(add_btn)
        layout.addWidget(btn_row)
        return widget

    def create_customer_flow_table(self):
        """Create Position Flow Summary table"""
        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.setContentsMargins(0, 0, 0, 0)
        
        # Header with controls
        header_widget = QWidget()
        header_layout = QHBoxLayout(header_widget)
        header_layout.setContentsMargins(8, 8, 8, 8)
        
        # Header label
        header = QLabel("Client Flow Summary (by Pair)")
        header.setStyleSheet("color: white; font-weight: bold; font-size: 14px;")
        header_layout.addWidget(header)
        
        # Search input
        search_label = QLabel("Search:")
        search_label.setStyleSheet("color: white; font-size: 12px; margin-right: 5px;")
        header_layout.addWidget(search_label)
        
        self.customer_flow_search = QLineEdit()
        self.customer_flow_search.setPlaceholderText("Type to search...")
        self.customer_flow_search.setFixedWidth(200)
        self.customer_flow_search.setStyleSheet("""
            QLineEdit {
                background-color: #1B2937; color: white; border: 1px solid #263544;
                padding: 4px 8px; border-radius: 3px; font-size: 11px;
            }
            QLineEdit:focus { border: 1px solid #ff9900; }
        """)
        self.customer_flow_search.textChanged.connect(
        lambda text: self.filter_table_by_text(self.customer_flow_table, text))
        header_layout.addWidget(self.customer_flow_search)
        
        header_layout.addStretch()
        
        layout.addWidget(header_widget)
        
        # Table
        self.customer_flow_table = QTableWidget()
        columns = [" Symbol ", "Total Amount", "Client Buy Amount", "Client Sell Amount", "Net Amount"]
        self.customer_flow_table.setColumnCount(len(columns))
        self.customer_flow_table.setHorizontalHeaderLabels(columns)
        self.setup_table_properties(self.customer_flow_table)
        self.customer_flow_table.setItemDelegate(PaddingDelegate(padding=6, parent=self.customer_flow_table))
        layout.addWidget(self.customer_flow_table)
        return widget

    def create_execution_summary_table(self):
        """Create Execution Summary table"""
        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.setContentsMargins(0, 0, 0, 0)
        
        # Header with controls
        header_widget = QWidget()
        header_layout = QHBoxLayout(header_widget)
        header_layout.setContentsMargins(8, 8, 8, 8)
        
        # Header label
        header = QLabel("Interbank Execution Summary")
        header.setStyleSheet("color: white; font-weight: bold; font-size: 14px;")
        header_layout.addWidget(header)
        
        # Search input
        search_label = QLabel("Search:")
        search_label.setStyleSheet("color: white; font-size: 12px; margin-right: 5px;")
        header_layout.addWidget(search_label)
        
        self.execution_summary_search = QLineEdit()
        self.execution_summary_search.setPlaceholderText("Type to search...")
        self.execution_summary_search.setFixedWidth(200)
        self.execution_summary_search.setStyleSheet("""
            QLineEdit {
                background-color: #1B2937; color: white; border: 1px solid #263544;
                padding: 4px 8px; border-radius: 3px; font-size: 11px;
            }
            QLineEdit:focus { border: 1px solid #ff9900; }
        """)
        self.execution_summary_search.textChanged.connect(
        lambda text: self.filter_table_by_text(self.execution_summary_table, text))
        header_layout.addWidget(self.execution_summary_search)
        
        header_layout.addStretch()
        
        layout.addWidget(header_widget)
        
        # Table
        self.execution_summary_table = QTableWidget()
        columns = [" Symbol ", "Total Amount", "Total Buy Amount", "Total Sell Amount", "Net Amount"]
        self.execution_summary_table.setColumnCount(len(columns))
        self.execution_summary_table.setHorizontalHeaderLabels(columns)
        self.setup_table_properties(self.execution_summary_table)
        self.execution_summary_table.setItemDelegate(PaddingDelegate(padding=6, parent=self.execution_summary_table))
        layout.addWidget(self.execution_summary_table)
        return widget

    def create_currency_flow_table(self):
        """Create Currency Flow Summary table"""
        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.setContentsMargins(0, 0, 0, 0)
        
        # Header with controls
        header_widget = QWidget()
        header_layout = QHBoxLayout(header_widget)
        header_layout.setContentsMargins(8, 8, 8, 8)
        
        # Header label
        header = QLabel("Client Flow Summary (by Currency)")
        header.setStyleSheet("color: white; font-weight: bold; font-size: 14px;")
        header_layout.addWidget(header)
        
        # Search input
        search_label = QLabel("Search:")
        search_label.setStyleSheet("color: white; font-size: 12px; margin-right: 5px;")
        header_layout.addWidget(search_label)
        
        self.currency_flow_search = QLineEdit()
        self.currency_flow_search.setPlaceholderText("Type to search...")
        self.currency_flow_search.setFixedWidth(200)
        self.currency_flow_search.setStyleSheet("""
            QLineEdit {
                background-color: #1B2937; color: white; border: 1px solid #263544;
                padding: 4px 8px; border-radius: 3px; font-size: 11px;
            }
            QLineEdit:focus { border: 1px solid #ff9900; }
        """)
        self.currency_flow_search.textChanged.connect(
        lambda text: self.filter_table_by_text(self.currency_flow_table, text))
        header_layout.addWidget(self.currency_flow_search)
        
        header_layout.addStretch()
        
        layout.addWidget(header_widget)
        
        # Table
        self.currency_flow_table = QTableWidget()
        columns = ["Currency", "Total Buy Amount", "Total Sell Amount", "Net Amount"]
        self.currency_flow_table.setColumnCount(len(columns))
        self.currency_flow_table.setHorizontalHeaderLabels(columns)
        self.setup_table_properties(self.currency_flow_table)
        self.currency_flow_table.setItemDelegate(PaddingDelegate(padding=6, parent=self.currency_flow_table))
        layout.addWidget(self.currency_flow_table)
        return widget

    def create_risk_summary_table(self):
        """Create Risk Summary table"""
        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.setContentsMargins(0, 0, 0, 0)
        
        # Header with controls
        header_widget = QWidget()
        header_layout = QHBoxLayout(header_widget)
        header_layout.setContentsMargins(8, 8, 8, 8)
        
        # Header label
        header = QLabel("Risk Summary")
        header.setStyleSheet("color: white; font-weight: bold; font-size: 14px;")
        header_layout.addWidget(header)
        
        # Search input
        search_label = QLabel("Search:")
        search_label.setStyleSheet("color: white; font-size: 12px; margin-right: 5px;")
        header_layout.addWidget(search_label)
        
        self.risk_summary_search = QLineEdit()
        self.risk_summary_search.setPlaceholderText("Type to search...")
        self.risk_summary_search.setFixedWidth(200)
        self.risk_summary_search.setStyleSheet("""
            QLineEdit {
                background-color: #1B2937; color: white; border: 1px solid #263544;
                padding: 4px 8px; border-radius: 3px; font-size: 11px;
            }
            QLineEdit:focus { border: 1px solid #ff9900; }
        """)
        self.risk_summary_search.textChanged.connect(
        lambda text: self.filter_table_by_text(self.risk_summary_table, text))
        header_layout.addWidget(self.risk_summary_search)
        
        header_layout.addStretch()
        
        layout.addWidget(header_widget)
        
        # Table
        self.risk_summary_table = QTableWidget()
        columns = [" Symbol ", "Price Feed\nStatus", "Strategy\nStatus", "Position\nControl", "Last Order\nStatus", "Risk\nScore"]
        self.risk_summary_table.setColumnCount(len(columns))
        self.risk_summary_table.setHorizontalHeaderLabels(columns)
        self.setup_table_properties(self.risk_summary_table)
        layout.addWidget(self.risk_summary_table)
        return widget

    def create_position_table(self):
        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.setContentsMargins(0, 0, 0, 0)
        # ---------------- Table ---------------- #
        self.position_table = QTableWidget()
        columns = [" Symbol ", "Position", "Position Value", "Average\nCost Rate", "Unrealized\nPNL",
                   "  Realized   \nPNL", "    Bid   ", "    Ask   ", "Bid\nVenue", "Ask\nVenue", "Close\nPosition"]
        self.position_table.setColumnCount(len(columns))
        self.position_table.setHorizontalHeaderLabels(columns)
        self.setup_table_properties(self.position_table)
        self.position_table.setItemDelegate(PaddingDelegate(padding=6, parent=self.position_table))
 
        # ---------------- Header with controls ---------------- #
        header_widget = QWidget()
        header_layout = QHBoxLayout(header_widget)
        header_layout.setContentsMargins(8, 8, 8, 8)
        # Header label
        header = QLabel("Position and Price")
        header.setStyleSheet("color: white; font-weight: bold; font-size: 14px;")
        header_layout.addWidget(header)
        # Search input (Execution Table’daki stil ile)
        search_label = QLabel("Search:")
        search_label.setStyleSheet("color: white; font-size: 12px; margin-right: 5px;")
        header_layout.addWidget(search_label)
        self.position_search = QLineEdit()
        self.position_search.setPlaceholderText("Type to search...")
        self.position_search.setFixedWidth(200)
        self.position_search.setStyleSheet(
            "background-color: #1B2937; color: white; border: 1px solid #263544; "
            "padding: 4px 8px; border-radius: 3px; font-size: 11px;"
        )
        self.position_search.textChanged.connect(
            lambda text: self.filter_table_by_text(self.position_table, text)
        )
        header_layout.addWidget(self.position_search)
        header_layout.addStretch()
        # ---------------- Filter ---------------- #
        self.position_filter = FilterWidget(self.position_table, widget)
        self.position_filter.toggle_btn.setFixedHeight(24)
        self.position_filter.toggle_btn.setStyleSheet("font-size: 11px; padding: 2px 8px;")
        # Filter butonu sağda
        header_layout.addWidget(self.position_filter.toggle_btn)

	
        # ---------------- Close All Positions Button ---------------- #
        self.close_all_button = QPushButton("Close All Positions")
        self.close_all_button.setFixedHeight(24)
        self.close_all_button.setStyleSheet(f"""
            QPushButton {{
                background-color: {color_theme}; color: white; border: 1px solid {color_theme};
                padding: 2px 4px; border-radius: 2px; font-size: 10px; font-weight: bold;
            }}
            QPushButton:hover {{ background-color: {color_theme_hover}; }}
            QPushButton:pressed {{ background-color: {color_theme_pressed}; }}
        """)
        self.close_all_button.clicked.connect(self.close_all_positions)
        header_layout.addWidget(self.close_all_button)
        
        layout.addWidget(header_widget)
        # Filter frame header ile table arasında
        layout.addWidget(self.position_filter.frame)
        layout.addWidget(self.position_table)
        return widget

    def create_strategy_table(self):
        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.setContentsMargins(0, 0, 0, 0)
        
        # Header with controls
        header_widget = QWidget()
        header_layout = QHBoxLayout(header_widget)
        header_layout.setContentsMargins(8, 8, 8, 8)
        
        # Header label
        header = QLabel("Strategy")
        header.setStyleSheet("color: white; font-weight: bold; font-size: 14px;")
        header_layout.addWidget(header)
        
        # Search input
        search_label = QLabel("Search:")
        search_label.setStyleSheet("color: white; font-size: 12px; margin-right: 5px;")
        header_layout.addWidget(search_label)
        
        self.strategy_search = QLineEdit()
        self.strategy_search.setPlaceholderText("Type to search...")
        self.strategy_search.setFixedWidth(200)
        self.strategy_search.setStyleSheet("""
            QLineEdit {
                background-color: #1B2937; color: white; border: 1px solid #263544;
                padding: 4px 8px; border-radius: 3px; font-size: 11px;
            }
            QLineEdit:focus { border: 1px solid #ff9900; }
        """)
        self.strategy_search.textChanged.connect(
        lambda text: self.filter_table_by_text(self.strategy_table, text))
        header_layout.addWidget(self.strategy_search)
        
        header_layout.addStretch()
        
        # Start/Stop buttons
        self.start_all_btn = QPushButton("START ALL")
        self.start_all_btn.setFixedSize(80, 25)
        self.start_all_btn.setStyleSheet(f"""
            QPushButton {{
                background-color: {color_success} ; color: white; border: 1px solid {color_success_hover};
                padding: 2px 4px; border-radius: 3px; font-weight: bold; font-size: 10px;
            }}
            QPushButton:hover {{ background-color: {color_success_hover}; }}
            QPushButton:pressed {{ background-color: {color_success_press}; }}
        """)
        self.start_all_btn.clicked.connect(self.start_all_strategies)
        header_layout.addWidget(self.start_all_btn)
        
        self.stop_all_btn = QPushButton("STOP ALL")
        self.stop_all_btn.setFixedSize(80, 25)
        self.stop_all_btn.setStyleSheet("""
            QPushButton {
                background-color: #FF7B63; color: white; border: 1px solid #DA6857;
                padding: 2px 4px; border-radius: 3px; font-weight: bold; font-size: 10px;
            }
            QPushButton:hover { background-color: #DA6857; }
            QPushButton:pressed { background-color: #B4554B; }
        """)
        self.stop_all_btn.clicked.connect(self.stop_all_strategies)
        header_layout.addWidget(self.stop_all_btn)
        
        layout.addWidget(header_widget)
       
        # Table
        self.strategy_table = QTableWidget()
        columns = ["Start\nStop", " Symbol ", "Status", "Transfer\nAmount", "Long Position\nLimit",
                   "Short Position\nLimit", "Min\nHedge", "Hedge\nRatio", "Take\nProfit",
                   "Stop\nLoss", "Spread", "   Edit   "]
        self.strategy_table.setColumnCount(len(columns))
        self.strategy_table.setHorizontalHeaderLabels(columns)
        self.setup_strategy_table_properties()
        self.strategy_table.setItemDelegate(PaddingDelegate(padding=6, parent=self.strategy_table))
        # Don't populate immediately - wait for delayed population
        layout.addWidget(self.strategy_table)
        return widget
   
    def create_provider_table(self):
        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.setContentsMargins(0, 0, 0, 0)
        
        # Header with controls
        header_widget = QWidget()
        header_layout = QHBoxLayout(header_widget)
        header_layout.setContentsMargins(8, 8, 8, 8)
        
        # Header label
        header = QLabel("Providers")
        header.setStyleSheet("color: white; font-weight: bold; font-size: 14px;")
        header_layout.addWidget(header)
        
        # Search input
        search_label = QLabel("Search:")
        search_label.setStyleSheet("color: white; font-size: 12px; margin-right: 5px;")
        header_layout.addWidget(search_label)
        
        self.provider_search = QLineEdit()
        self.provider_search.setPlaceholderText("Type to search...")
        self.provider_search.setFixedWidth(200)
        self.provider_search.setStyleSheet("""
            QLineEdit {
                background-color: #1B2937; color: white; border: 1px solid #263544;
                padding: 4px 8px; border-radius: 3px; font-size: 11px;
            }
            QLineEdit:focus { border: 1px solid #ff9900; }
        """)
        self.provider_search.textChanged.connect(
        lambda text: self.filter_table_by_text(self.provider_table, text))
        header_layout.addWidget(self.provider_search)
        
        header_layout.addStretch()
        layout.addWidget(header_widget)
        
        # Table
        self.provider_table = QTableWidget()
        columns = [" Symbol ", "   KFH   ", "   Integral   ", "   Tradair   ", "   360T   "]
        self.provider_table.setColumnCount(len(columns))
        self.provider_table.setHorizontalHeaderLabels(columns)
        self.setup_table_properties(self.provider_table)
        self.provider_table.setItemDelegate(PaddingDelegate(padding=6, parent=self.provider_table))
        # Don't populate immediately - wait for delayed population
        layout.addWidget(self.provider_table)
        return widget

    def create_execution_table(self):
        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.setContentsMargins(0, 0, 0, 0)
        
        # Header with controls
        header_widget = QWidget()
        header_layout = QHBoxLayout(header_widget)
        header_layout.setContentsMargins(8, 8, 8, 8)
        
        # Header label
        header = QLabel("Execution Results")
        header.setStyleSheet("color: white; font-weight: bold; font-size: 14px;")
        header_layout.addWidget(header)
        
        # Search input
        search_label = QLabel("Search:")
        search_label.setStyleSheet("color: white; font-size: 12px; margin-right: 5px;")
        header_layout.addWidget(search_label)
        
        self.execution_search = QLineEdit()
        self.execution_search.setPlaceholderText("Type to search...")
        self.execution_search.setFixedWidth(200)
        self.execution_search.setStyleSheet("""
            QLineEdit {
                background-color: #1B2937; color: white; border: 1px solid #263544;
                padding: 4px 8px; border-radius: 3px; font-size: 11px;
            }
            QLineEdit:focus { border: 1px solid #ff9900; }
        """)
        self.execution_search.textChanged.connect(
        lambda text: self.filter_table_by_text(self.execution_table, text))
        header_layout.addWidget(self.execution_search)
        
        header_layout.addStretch()
        
        layout.addWidget(header_widget)
        
        # Table
        self.execution_table = QTableWidget()
        columns = [" Symbol ", "Quantity", " Price ", "Side", "Venue", "    Counter Party    ", "         Timestamp         ", "    Strategy    ", "Realized PnL"]
        self.execution_table.setColumnCount(len(columns))
        self.execution_table.setHorizontalHeaderLabels(columns)
        self.setup_table_properties(self.execution_table)
        self.execution_table.setItemDelegate(PaddingDelegate(padding=6, parent=self.execution_table))
                # ---------------- Filter ---------------- #
        self.execution_filter = FilterWidget(self.execution_table, widget)
        self.execution_filter.toggle_btn.setFixedHeight(24)
        self.execution_filter.toggle_btn.setStyleSheet("font-size: 11px; padding: 2px 8px;")
                # Filter butonu sağda
        header_layout.addWidget(self.execution_filter.toggle_btn)
        layout.addWidget(header_widget)
        # Filter frame header ile table arasında
        layout.addWidget(self.execution_filter.frame)

        # Don't populate immediately - wait for delayed population
        layout.addWidget(self.execution_table)
        return widget
    
    def create_customer_transaction_table(self):
        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.setContentsMargins(0, 0, 0, 0)
        
        # Header with controls
        header_widget = QWidget()
        header_layout = QHBoxLayout(header_widget)
        header_layout.setContentsMargins(8, 8, 8, 8)
        
        # Header label
        header = QLabel("Customer Transactions")
        header.setStyleSheet("color: white; font-weight: bold; font-size: 14px;")
        header_layout.addWidget(header)
        
        # Search input
        search_label = QLabel("Search:")
        search_label.setStyleSheet("color: white; font-size: 12px; margin-right: 5px;")
        header_layout.addWidget(search_label)
        
        self.customer_transaction_search = QLineEdit()
        self.customer_transaction_search.setPlaceholderText("Type to search...")
        self.customer_transaction_search.setFixedWidth(200)
        self.customer_transaction_search.setStyleSheet("""
            QLineEdit {
                background-color: #1B2937; color: white; border: 1px solid #263544;
                padding: 4px 8px; border-radius: 3px; font-size: 11px;
            }
            QLineEdit:focus { border: 1px solid #ff9900; }
        """)
        self.customer_transaction_search.textChanged.connect(
        lambda text: self.filter_table_by_text(self.customer_transaction_table, text))
        header_layout.addWidget(self.customer_transaction_search)
        
        header_layout.addStretch()
        
        layout.addWidget(header_widget)
        
        # Table
        self.customer_transaction_table = QTableWidget()
        columns = [" Symbol ","Quantity","       Side       ","Tran Price","          Timestamp          ","Customer ID","Bid Price","Ask Price","Sales\nPnL","    Tran ID    "]
        self.customer_transaction_table.setColumnCount(len(columns))
        self.customer_transaction_table.setHorizontalHeaderLabels(columns)
        self.setup_table_properties(self.customer_transaction_table)
        self.customer_transaction_table.setItemDelegate(PaddingDelegate(padding=6, parent=self.customer_transaction_table))
        # ---------------- Filter ---------------- #
        self.customer_transaction_filter = FilterWidget(self.customer_transaction_table, widget)
        self.customer_transaction_filter.toggle_btn.setFixedHeight(24)
        self.customer_transaction_filter.toggle_btn.setStyleSheet("font-size: 11px; padding: 2px 8px;")
        # Filter butonu sağda
        header_layout.addWidget(self.customer_transaction_filter.toggle_btn)
        layout.addWidget(header_widget)
        # Filter frame header ile table arasında
        layout.addWidget(self.customer_transaction_filter.frame)
        # Don't populate immediately - wait for delayed population
        layout.addWidget(self.customer_transaction_table)
        return widget

    def create_internal_transaction_table(self):
        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.setContentsMargins(0, 0, 0, 0)
        
        # Header with controls
        header_widget = QWidget()
        header_layout = QHBoxLayout(header_widget)
        header_layout.setContentsMargins(8, 8, 8, 8)
        
        # Header label
        header = QLabel("Internal Trades")
        header.setStyleSheet("color: white; font-weight: bold; font-size: 14px;")
        header_layout.addWidget(header)
        
        # Search input
        search_label = QLabel("Search:")
        search_label.setStyleSheet("color: white; font-size: 12px; margin-right: 5px;")
        header_layout.addWidget(search_label)
        
        self.internal_transaction_search = QLineEdit()
        self.internal_transaction_search.setPlaceholderText("Type to search...")
        self.internal_transaction_search.setFixedWidth(200)
        self.internal_transaction_search.setStyleSheet("""
            QLineEdit {
                background-color: #1B2937; color: white; border: 1px solid #263544;
                padding: 4px 8px; border-radius: 3px; font-size: 11px;
            }
            QLineEdit:focus { border: 1px solid #ff9900; }
        """)
        self.internal_transaction_search.textChanged.connect(
        lambda text: self.filter_table_by_text(self.internal_transaction_table, text))
        header_layout.addWidget(self.internal_transaction_search)

        # Don't populate immediately - wait for delayed population
        header_layout.addStretch()
        
        layout.addWidget(header_widget)
        
        # Table
        self.internal_transaction_table = QTableWidget()
        columns = [" Symbol ","Quantity","       Side       ","Tran Price","          Timestamp          ","    Entity    "]
        self.internal_transaction_table.setColumnCount(len(columns))
        self.internal_transaction_table.setHorizontalHeaderLabels(columns)
        self.setup_table_properties(self.internal_transaction_table)
        self.internal_transaction_table.setItemDelegate(PaddingDelegate(padding=6, parent=self.internal_transaction_table))
        
        # ---------------- Filter ---------------- #
        self.internal_transaction_filter = FilterWidget(self.internal_transaction_table, widget)
        self.internal_transaction_filter.toggle_btn.setFixedHeight(24)
        self.internal_transaction_filter.toggle_btn.setStyleSheet("font-size: 11px; padding: 2px 8px;")
        # Filter butonu sağda
        header_layout.addWidget(self.internal_transaction_filter.toggle_btn)
        layout.addWidget(header_widget)
        # Filter frame header ile table arasında
        layout.addWidget(self.internal_transaction_filter.frame)

        # Don't populate immediately - wait for delayed population
        layout.addWidget(self.internal_transaction_table)
        return widget

    def create_manual_trade_table(self):
        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.setContentsMargins(0, 0, 0, 0)
        
        # Header with controls
        header_widget = QWidget()
        header_layout = QHBoxLayout(header_widget)
        header_layout.setContentsMargins(8, 8, 8, 8)
        
        # Header label
        header = QLabel("Manual Trade List")
        header.setStyleSheet("color: white; font-weight: bold; font-size: 14px;")
        header_layout.addWidget(header)
        
        # Search input
        search_label = QLabel("Search:")
        search_label.setStyleSheet("color: white; font-size: 12px; margin-right: 5px;")
        header_layout.addWidget(search_label)
        
        self.manual_trade_search = QLineEdit()
        self.manual_trade_search.setPlaceholderText("Type to search...")
        self.manual_trade_search.setFixedWidth(200)
        self.manual_trade_search.setStyleSheet("""
            QLineEdit {
                background-color: #1B2937; color: white; border: 1px solid #263544;
                padding: 4px 8px; border-radius: 3px; font-size: 11px;
            }
            QLineEdit:focus { border: 1px solid #ff9900; }
        """)
        self.manual_trade_search.textChanged.connect(
        lambda text: self.filter_table_by_text(self.manual_trade_table, text))
        header_layout.addWidget(self.manual_trade_search)
        
        header_layout.addStretch()
    
        layout.addWidget(header_widget)
        
        # Table
        self.manual_trade_table = QTableWidget()
        columns = [" Symbol ", "Quantity", "Price", "Side", "Venue", "    Counter Party    ", "         Timestamp         ", "    Strategy    ", "Realized PnL"]
        self.manual_trade_table.setColumnCount(len(columns))
        self.manual_trade_table.setHorizontalHeaderLabels(columns)
        self.setup_table_properties(self.manual_trade_table)
        self.manual_trade_table.setItemDelegate(PaddingDelegate(padding=6, parent=self.manual_trade_table))

        # Don't populate immediately - wait for delayed population
        layout.addWidget(self.manual_trade_table)
        return widget

    def create_risk_table(self):
        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.setContentsMargins(0, 0, 0, 0)
        
        # Header with controls
        header_widget = QWidget()
        header_layout = QHBoxLayout(header_widget)
        header_layout.setContentsMargins(8, 8, 8, 8)
        
        # Header label
        header = QLabel("Risk Monitor")
        header.setStyleSheet("color: white; font-weight: bold; font-size: 14px;")
        header_layout.addWidget(header)
        
        # Search input
        search_label = QLabel("Search:")
        search_label.setStyleSheet("color: white; font-size: 12px; margin-right: 5px;")
        header_layout.addWidget(search_label)
        
        self.risk_search = QLineEdit()
        self.risk_search.setPlaceholderText("Type to search...")
        self.risk_search.setFixedWidth(200)
        self.risk_search.setStyleSheet("""
            QLineEdit {
                background-color: #1B2937; color: white; border: 1px solid #263544;
                padding: 4px 8px; border-radius: 3px; font-size: 11px;
            }
            QLineEdit:focus { border: 1px solid #ff9900; }
        """)
        self.risk_search.textChanged.connect(
        lambda text: self.filter_table_by_text(self.risk_table, text))
        header_layout.addWidget(self.risk_search)
        
        header_layout.addStretch()
        
        layout.addWidget(header_widget)
        
        # Table
        self.risk_table = QTableWidget()
        columns = [" Symbol ", "KFH Feed\n   Status   ", "Integral Feed\nStatus", "Tradair Feed\nStatus", "360T Feed\nStatus", "Strategy\nStatus", "Position\nControl",
                   "Last Order\nStatus", "Spread\nCheck", "Position Flow\nCheck"]
        self.risk_table.setColumnCount(len(columns))
        self.risk_table.setHorizontalHeaderLabels(columns)
        self.setup_table_properties(self.risk_table)
        # Don't populate immediately - wait for delayed population

        # ---------------- Filter ---------------- #
        self.risk_filter = FilterWidget(self.risk_table, widget)
        self.risk_filter.toggle_btn.setFixedHeight(24)
        self.risk_filter.toggle_btn.setStyleSheet("font-size: 11px; padding: 2px 8px;")
                # Filter butonu sağda
        header_layout.addWidget(self.risk_filter.toggle_btn)
        layout.addWidget(header_widget)
        # Filter frame header ile table arasında
        layout.addWidget(self.risk_filter.frame)

        # Don't populate immediately - wait for delayed population
        layout.addWidget(self.risk_table)
        return widget
    
#Setup Properties

    def setup_table_properties(self, table):
        table.setAlternatingRowColors(True)
        table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)  # Hücre yerine satır seç
        table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        table.setSortingEnabled(True)
        table.setShowGrid(True)
        header = table.horizontalHeader()
        
        # Set header height to accommodate two-line text
        header.setDefaultSectionSize(40)
        
        # Set column widths based on content to ensure headers are fully visible
        for i in range(table.columnCount()):
            header.setSectionResizeMode(i, QHeaderView.ResizeMode.Interactive)
            # Set minimum width to ensure headers are visible
            table.setColumnWidth(i, 110)
        
        # Ensure headers are visible and properly sized
        header.setStretchLastSection(False)
        header.setDefaultAlignment(Qt.AlignmentFlag.AlignCenter)
        self._attach_focus_behavior(table)
        table.verticalHeader().setDefaultSectionSize(28)
        table.verticalHeader().setVisible(False)
        table.setStyleSheet("""
           QScrollBar:vertical {
               background: #0A1929;
               width: 10px;
               margin: 2px 0 2px 0;
               border: none;
           }
           QScrollBar::handle:vertical {
               background: #2A3A4D;
               min-height: 20px;
               border-radius: 5px;
           }
           QScrollBar::handle:vertical:hover {
               background: #3A4C5E;
           }
           QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
               background: none;
               height: 0px;
           }
           QScrollBar::add-page:vertical, QScrollBar::sub-page:vertical {
               background: none;
           }
           QScrollBar:horizontal {
               background: #0A1929;
               height: 10px;
               margin: 0 2px 0 2px;       /* sağ-sol boşluk */
               border: none;
           }
           QScrollBar::handle:horizontal {
               background: #2A3A4D;
               min-width: 20px;
               border-radius: 5px;
           }
           QScrollBar::handle:horizontal:hover {
               background: #3A4C5E;
           }
           QScrollBar::add-line:horizontal,
           QScrollBar::sub-line:horizontal {
               width: 0px;                /* ok düğmelerini gizle */
               background: none;
           }
           QScrollBar::add-page:horizontal,
           QScrollBar::sub-page:horizontal {
               background: none;
           }
       """)
        table.setVerticalScrollMode(QAbstractItemView.ScrollMode.ScrollPerPixel)
        table.setHorizontalScrollMode(QAbstractItemView.ScrollMode.ScrollPerPixel)
        table.setItemDelegate(HoverDelegate(table))
        font = QFont("Roboto Mono",10)
        table.setFont(font)
        table.horizontalHeader().setFont(font)
        ##        table.setItemDelegate(PaddingDelegate(padding=6, parent=table))
        
    def setup_strategy_table_properties(self):
        """Setup strategy table with no selection highlighting and centered sliders"""
        self.strategy_table.setAlternatingRowColors(True)
        self.strategy_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)  # Hücre yerine satır seç
        self.strategy_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)  # Disable editing initially
        self.strategy_table.setSortingEnabled(True)
        self.strategy_table.setShowGrid(True)
        self.strategy_table.setSelectionMode(QTableWidget.SelectionMode.NoSelection)  # Disable selection
        header = self.strategy_table.horizontalHeader()
        
        # Set column widths to ensure headers are fully visible
        for i in range(self.strategy_table.columnCount()):
            header.setSectionResizeMode(i, QHeaderView.ResizeMode.Interactive)
            # Set minimum width to ensure headers are visible
            self.strategy_table.setColumnWidth(i, 110)
        
        # Ensure headers are visible and properly sized
        header.setStretchLastSection(False)
        header.setDefaultAlignment(Qt.AlignmentFlag.AlignCenter)
        
        # Set header height to accommodate two-line text
        header.setDefaultSectionSize(40)
        
        self.strategy_table.verticalHeader().setDefaultSectionSize(28)
        self.strategy_table.verticalHeader().setVisible(False)
        
        # Make status column (Column 2) non-editable
        self.strategy_table.setItemDelegateForColumn(2, NonEditableDelegate())
        self.strategy_table.setStyleSheet("""
           QScrollBar:vertical {
               background: #0A1929;
               width: 10px;
               margin: 2px 0 2px 0;
               border: none;
           }
           QScrollBar::handle:vertical {
               background: #2A3A4D;
               min-height: 20px;
               border-radius: 5px;
           }
           QScrollBar::handle:vertical:hover {
               background: #3A4C5E;
           }
           QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
               background: none;
               height: 0px;
           }
           QScrollBar::add-page:vertical, QScrollBar::sub-page:vertical {
               background: none;
           }
           QScrollBar:horizontal {
               background: #0A1929;
               height: 10px;
               margin: 0 2px 0 2px;       /* sağ-sol boşluk */
               border: none;
           }
           QScrollBar::handle:horizontal {
               background: #2A3A4D;
               min-width: 20px;
               border-radius: 5px;
           }
           QScrollBar::handle:horizontal:hover {
               background: #3A4C5E;
           }
           QScrollBar::add-line:horizontal,
           QScrollBar::sub-line:horizontal {
               width: 0px;                /* ok düğmelerini gizle */
               background: none;
           }
           QScrollBar::add-page:horizontal,
           QScrollBar::sub-page:horizontal {
               background: none;
           }
        """)
        self.strategy_table.setVerticalScrollMode(QAbstractItemView.ScrollMode.ScrollPerPixel)
        self.strategy_table.setHorizontalScrollMode(QAbstractItemView.ScrollMode.ScrollPerPixel)
        self._attach_focus_behavior(self.strategy_table)
        font = QFont("Roboto Mono",10)
        self.strategy_table.setFont(font)
        self.strategy_table.horizontalHeader().setFont(font)

    def update_strategy_status(self, row, switch):
        symbol = self.strategy_table.item(row, 1).text()
        # Kafka cevabını bekliyorsak işlemi iptal et
        if self.waiting_for_strategy_kafka_confirmation:
            print(f"DEBUG: Ignoring switch toggle for {symbol} - waiting for Kafka confirmation")
            return
        # Yeni durum: Switch açık → Running, Kapalı → Stopped
        status_string = "Running" if switch._checked else "Stopped"
        print(f"DEBUG: update_strategy_status called - row={row}, switch_checked={switch._checked}, status_string={status_string}")
        # Kafka bekleme flag’ini devreye al
        self.waiting_for_strategy_kafka_confirmation = True
        if symbol in self.strategy_data:
            self.strategy_data[symbol]['status'] = status_string
            self.pending_strategy_confirmations.add(symbol)
            print(f"DEBUG: Updated strategy data for {symbol} - status={status_string}")
            self.send_strategy_update_to_kafka(symbol)
        else:
            print(f"Strategy data not found for {symbol}")
            self.waiting_for_strategy_kafka_confirmation = False

    def update_provider_status(self, row, column, switch):
    # Provider eşleme tabloları
        provider_map = {1: "KFH", 2: "Integral", 3: "Tradair", 4: "360T"}
        status_key_map = {
            "KFH": "kfh_gui_status",
            "Integral": "integral_gui_status",
            "Tradair": "tradair_gui_status",
            "360T": "360t_gui_status",
        }
        # Sembol hep ilk sütundan alınır
        item = self.provider_table.item(row, 0)
        if not item:
            print(f"DEBUG: No symbol at row {row}")
            return
        symbol = item.text()
        # Sadece provider sütunlarında çalış
        if column not in provider_map:
            print(f"DEBUG: Ignoring toggle in column {column} - not a provider column")
            return
        provider_name = provider_map[column]
        status_key = status_key_map[provider_name]
        # Kafka cevabını bekliyorsak işlemi iptal et
        if self.waiting_for_provider_kafka_confirmation:
            print(f"DEBUG: Ignoring switch toggle for {symbol}/{provider_name} - waiting for Kafka confirmation")
            return
        # Yeni durum (Strategy’deki mantıkla birebir)
        status_string = "True" if switch._checked else "False"
        print(f"DEBUG: update_provider_status called - row={row}, col={column}, "
                f"provider={provider_name}, symbol={symbol}, status={status_string}")
        # Kafka bekleme flag’i
        self.waiting_for_provider_kafka_confirmation = True
        # Provider datasını güncelle
        if symbol in self.provider_data:
            self.provider_data[symbol][status_key] = status_string
            self.pending_provider_confirmations.add((symbol, provider_name))
            print(f"DEBUG: Updated provider data for {symbol}/{provider_name} "
                    f"- {status_key}={status_string}")
            self.send_provider_update_to_kafka(symbol)  # tüm provider durumlarını gönder
        else:
            print(f"Provider data not found for {symbol}/{provider_name}")
            self.waiting_for_provider_kafka_confirmation = False
            

    def cleanup_data_structures(self):
        """Periodically clean up data structures to prevent duplicates and memory issues"""
        # Ensure unique execution data
        # self.ensure_unique_execution_data()
        self.ensure_unique_manual_trade_data()       
        
        # Keep only recent execution data (last 100 entries)
        if hasattr(self, 'execution_data_list') and len(self.execution_data_list) > 100:
            self.execution_data_list = self.execution_data_list[-100:]
            # print(f"Execution data trimmed to {len(self.execution_data_list)} entries")
        
        # Keep only recent manual trade data (last 100 entries)
        if hasattr(self, 'manual_trade_list') and len(self.manual_trade_list) > 100:
            self.manual_trade_list = self.manual_trade_list[-100:]
            # print(f"Manual trade data trimmed to {len(self.manual_trade_list)} entries")
       
        # Ensure all dictionary-based data structures are clean
        # (dictionaries with symbol keys should naturally prevent duplicates)
        #print(f"Data cleanup completed - Risk: {len(self.risk_data)}, Position: {len(self.position_data)}, Strategy: {len(self.strategy_data)}")

    def update_all_tables_from_data(self):
        """Update all tables from stored Kafka data at fixed intervals"""
        try:
            import time
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Clean up data structures first
            self.cleanup_data_structures()
            
            # Update position table
            if hasattr(self, 'position_table') and self.position_data:
                self.update_position_table()

            ##                print(f"[{current_time}] Updated position table with {len(self.position_data)} symbols")
                    
                    # Update risk table
            if hasattr(self, 'risk_table') and self.risk_data:
                self.update_risk_table()
            ##                print(f"[{current_time}] Updated risk table with {len(self.risk_data)} symbols")
                
            # Update strategy table
            if hasattr(self, 'strategy_table') and self.strategy_data:
                self.update_strategy_table()
            ##                print(f"[{current_time}] Updated strategy table with {len(self.strategy_data)} symbols")
            
            # Update provider table
            if hasattr(self, 'provider_table') and self.provider_data:
                self.update_provider_table()
            ##                print(f"[{current_time}] Updated provider table with {len(self.provider_data)} symbols")
             
            # Update execution table
            if hasattr(self, 'execution_table') and self.execution_data_list:
                self.update_execution_table()
            ##                print(f"[{current_time}] Updated execution table with {len(self.execution_data_list)} executions")
             
            # Update customer transaction table
            if hasattr(self, 'customer_transaction_table') and self.customer_transaction_data_list:
                self.update_customer_transaction_table()
            ##                print(f"[{current_time}] Updated customer transaction table with {len(self.customer_transaction_data_list)} customer_transactions")            

            # Update internal transaction table
            if hasattr(self, 'internal_transaction_table') and self.internal_transaction_data_list:
                self.update_internal_transaction_table()
            ##                print(f"[{current_time}] Updated internal transaction table with {len(self.internal_transaction_data_list)} customer_transactions")            
             
            # Update manual trade table
            if hasattr(self, 'manual_trade_table') and self.manual_trade_list:
                self.update_manual_trade_table()
            ##                print(f"[{current_time}] Updated manual trade table with {len(self.manual_trade_list)} manual trades")
 
            # Update overview tables
            if hasattr(self, 'position_pnl_table') and self.position_pnl_data:
                self.update_position_pnl_table()
            ##    print(f"[{current_time}] Updated position PNL table with {len(self.position_pnl_data)} symbols")
            
            if hasattr(self, 'customer_flow_table') and self.customer_flow_data:
                self.update_customer_flow_table()
            ##    print(f"[{current_time}] Updated position flow table with {len(self.customer_flow_data)} symbols")
            
            if hasattr(self, 'execution_summary_table') and self.execution_summary_data:
                self.update_execution_summary_table()
            ##    print(f"[{current_time}] Updated execution summary table with {len(self.execution_summary_data)} symbols")
            
            if hasattr(self, 'currency_flow_table') and self.currency_flow_data:
                self.update_currency_flow_table()
            ##    print(f"[{current_time}] Updated currency flow table with {len(self.currency_flow_data)} currencies")        
            
            # Update cumulative metrics
            self.update_cumulative_metrics()
            
        except Exception as e:
            print(f"Error updating tables: {e}")
            import traceback
            traceback.print_exc()

    def update_cumulative_metrics(self):
        """Update cumulative PnL and position values from position table data"""
        # Calculate cumulative PnL from realized PnL values in position table
        cumulative_pnl = 0
        cumulative_pos = 0
        
        for row in range(self.position_table.rowCount()):
            # Get realized PnL value
            realized_pnl_item = self.position_table.item(row, 5)  # Realized PnL column
            if realized_pnl_item:
                try:
                    # Extract numeric value from the item
                    pnl_text = realized_pnl_item.text().replace("$", "").replace(",", "")
                    pnl_value = float(pnl_text)
                    cumulative_pnl += pnl_value
                except ValueError:
                    continue
            
            # Get position value
            pos_value_item = self.position_table.item(row, 2)  # Position Value column
            if pos_value_item:
                try:
                    # Extract numeric value from the item
                    pos_text = pos_value_item.text().replace("$", "").replace(",", "")
                    pos_value = float(pos_text)
                    cumulative_pos += pos_value
                except ValueError:
                    continue
        
        # Update the display
        self.pnl_value.setText(f"$ {cumulative_pnl:,.2f}")
        self.pos_value.setText(f"$ {cumulative_pos:,.2f}")
        
        # Update color based on PnL value
        if cumulative_pnl > 0:
            self.pnl_value.setStyleSheet(f"color: #51C285 ; font-weight: bold; font-size: 10px;")
        else:
            self.pnl_value.setStyleSheet("color: #FF7B63; font-weight: bold; font-size: 10px;")

    def handle_kafka_update(self, topic, data):
        """Handle updates from Kafka - parsing data like kafka_table_viewer"""
        try:
            from datetime import datetime
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ##            print(f"[{current_time}] GUI received {topic}: {data}")
            
            if topic == 'PanelPosition_Sim2':
                fields = data.strip().split(",")
                if len(fields) < 12:
                    print("PanelPosition verisi eksik.")
                    return
                
                if fields[10].strip()=='Streambase':
                    symbol = fields[0].strip()
                    try:
                        self.position_data[symbol] = {
                            "position": float(fields[1]),
                            "position_value": abs(float(fields[2])),
                            "avg_cost_rate": float(fields[3]),
                            "unrealized_pnl": float(fields[4]),
                            "realized_pnl": float(fields[5]),
                            "bid": float(fields[6]),
                            "ask": float(fields[7]),
                            "bid_venue": fields[8].strip(),
                            "ask_venue": fields[9].strip(),
                            "producer": fields[10].strip(),
                            "system_date": fields[11].strip()
                        }
                    except Exception as e:
                        # print(f"Error parsing PanelPosition data: {e}")
                        # print(f"Fields: {fields}")
                        hasan=5
      
            elif topic == 'PanelExecutionResult_Sim2':
                fields = data.strip().split(",")
                if len(fields) < 8:
                    print("PanelExecutionResult verisi eksik.")
                    return
                if fields[9].strip()=='Streambase':
                    try:
                        execution = {
                            "symbol": fields[0].strip(),
                            "quantity": int(float(fields[1])),
                            "price": float(fields[2]),
                            "side": "Buy" if int(fields[3]) == 1 else "Sell",
                            "venue": fields[4].strip(),
                            "counterparty": fields[5].strip(),
                            "timestamp": fields[6].strip()[:-5],
                            "strategy": fields[7].strip(),
                            "realizedPnL": float(fields[8]),
                            "producer": fields[9].strip(),
                            "system_date": fields[10].strip()
                        }
                        if not hasattr(self, 'execution_data_list'):
                            self.execution_data_list = []
                        self.execution_data_list.append(execution)

                        # Eğer manual_trade_list yoksa oluştur
                        if not hasattr(self, 'manual_trade_list'):
                            self.manual_trade_list = []

                        # Eğer strategy 'Manual_Trade' ise ayrı listeye ekle
                        if execution.get("strategy", "") == "Manual_Trade":
                            self.manual_trade_list.append(execution)

                        # Ensure no duplicates in execution data
                        # self.ensure_unique_execution_data()
                    except Exception as e:
                        print(f"Error parsing PanelExecutionResult data: {e}")
                        print(f"Fields: {fields}")
     
            elif topic == 'PanelRiskMonitor_Sim2':
                fields = data.strip().split(",")
                if len(fields) < 12:
                    print("PanelRiskMonitor verisi eksik.")
                    return
                if fields[10].strip()=='Streambase':
                    symbol = fields[0].strip()
                    try:
                        # Strategy status comes as string "Running" or "Stopped"
                        strategy_status = fields[5].strip()
                        
                        self.risk_data[symbol] = {
                            "KFH_feed_status": fields[1].strip(),
                            "integral_feed_status": fields[2].strip(),
                            "tradair_feed_status": fields[3].strip(),
                            "360t_feed_status": fields[4].strip(),
                            "strategy_status": strategy_status,  # Keep as string
                            "position_control": fields[6].strip(),
                            "last_order_status": fields[7].strip(),
                            "spread_check": fields[8].strip(),
                            "customer_flow_check": fields[9].strip(),
                            "producer": fields[10].strip(),
                            "system_date": fields[11].strip()
                        }
                    except Exception as e:
                        print(f"Error parsing PanelRiskMonitor data: {e}")
                        print(f"Fields: {fields}")
        
            elif topic == 'PanelStrategy_Sim2':
                print(f"[{current_time}] GUI received {topic}: {data}")
                fields = data.strip().split(",")
                if len(fields) < 12:
                    print("PanelStrategy verisi eksik.")
                    return
                if fields[10].strip()=='Streambase':
                    symbol = fields[0].strip()
                    try:
                        # Strategy status comes as string "Running" or "Stopped"
                        strategy_status = fields[1].strip()
                        
                    ##                    spread_check_raw = fields[9].strip()
                    ##                    if '%' in spread_check_raw:
                    ##                        spread_check = int(float(spread_check_raw.replace('%', '')))
                    ##                    else:
                    ##                        spread_check = int(float(spread_check_raw))
                    ##                    
                        self.strategy_data[symbol] = {
                            "status": strategy_status,  # Keep as string
                            "transfer_amc": int(float(fields[2])),
                            "long_limit": int(float(fields[3])),
                            "short_limit": int(float(fields[4])),
                            "min_hedge": int(float(fields[5])),
                            "hedge_ratio": float(fields[6]),
                            "take_profit": float(fields[7]),
                            "stop_loss": float(fields[8]),
                            "spread_check": float(fields[9]),
                            "producer": fields[10].strip(),
                            "system_date": fields[11].strip()
                        }
                        
                        # Update the strategy table GUI with the confirmed status from Kafka
                        self.update_strategy_table_gui_from_kafka(symbol, strategy_status)
                        
                        if not hasattr(self, 'strategy_data_initialized') and len(self.strategy_data) == 8:
                            self.strategy_data_initialized = True
                            print("All strategy data initialized from Kafka")
                            
                    except Exception as e:
                        print(f"Error parsing PanelStrategy data: {e}")
                        print(f"Fields: {fields}")
            
            elif topic == "PanelProvider_Sim2":
                fields = data.strip().split(",")
                if len(fields) < 7:
                    print("PanelProvider verisi eksik:", fields)
                    return
                if fields[5].strip()=='Streambase':
                    symbol = fields[0].strip()
                    KFH_GUI_status = fields[1].strip()
                    Integral_GUI_status = fields[2].strip()
                    Tradair_GUI_status = fields[3].strip()
                    T360_GUI_status = fields[4].strip()
                    self.provider_data[symbol] = {
                        "kfh_gui_status": KFH_GUI_status,
                        "integral_gui_status": Integral_GUI_status,
                        "tradair_gui_status": Tradair_GUI_status,
                        "360t_gui_status": T360_GUI_status,
                        "producer": fields[5].strip(),
                        "system_date": fields[6].strip()
                    }
                    print(f"DEBUG provider_data[{symbol}] -> {self.provider_data[symbol]}")
                    # GUI güncelle (tek tek sütunlara bas)
                    self.update_provider_table_gui_from_kafka(symbol, "KFH", KFH_GUI_status)
                    self.update_provider_table_gui_from_kafka(symbol, "Integral", Integral_GUI_status)
                    self.update_provider_table_gui_from_kafka(symbol, "Tradair", Tradair_GUI_status)
                    self.update_provider_table_gui_from_kafka(symbol, "360T", T360_GUI_status)
                    # Flag reset kontrol
                    if not getattr(self, 'pending_provider_confirmations', []):
                        self.waiting_for_provider_kafka_confirmation = False
                        print("DEBUG: Provider confirmation complete, reset waiting flag (handle_kafka_update).")

            elif topic == 'PanelCustomerData_Sim2':
                fields = data.strip().split(",")
                if len(fields) < 12:
                    print("PanelCustomerData verisi eksik.")
                    return
                if fields[10].strip()=='Streambase':
                    try:
                        customer_transaction = {
                            "symbol": fields[0].strip(),
                            "quantity": float(fields[1]),
                            "side":  fields[2].strip(),
                            "tranprice": float(fields[3]),
                            "timestamp": fields[4].strip()[:-5],
                            "customerid": fields[5].strip(),
                            "bidprice": float(fields[6]),
                            "askprice": float(fields[7]),
                            "salespnl":  fields[8].strip(),
                            "tranid":  fields[9].strip(),
                            "producer": fields[10].strip(),
                            "system_date": fields[11].strip()
                        }
                        if not hasattr(self, 'customer_transaction_data_list'):
                            self.customer_transaction_data_list = []
                        self.customer_transaction_data_list.append(customer_transaction)
                        # Ensure no duplicates in execution data
                        self.ensure_unique_customer_transaction_data()

                    except Exception as e:
                        print(f"Error parsing PanelCustomerData data: {e}")
                        print(f"Fields: {fields}")
    
            elif topic == 'PanelCustomerData_Sim2_InternalTrade':
                fields = data.strip().split(",")
                print(f"Fields_InternalTrade: {fields}")
                if len(fields) < 10:
                    print("PanelCustomerData_Internal verisi eksik.")
                    return
                if fields[10].strip()=='Streambase':
                    try:
                        internal_transaction = {
                            "symbol": fields[0].strip(),
                            "quantity": float(fields[1]),
                            "side":  fields[2].strip(),
                            "tranprice": float(fields[3]),
                            "timestamp": fields[4].strip()[:-5],
                            "entity":  fields[5].strip(),
                            "producer": fields[6].strip(),
                            "system_date": fields[7].strip()
                        }
                        if not hasattr(self, 'internal_transaction_data_list'):
                            self.internal_transaction_data_list = []
                        self.internal_transaction_data_list.append(internal_transaction)
                        # Ensure no duplicates in execution data
                        self.ensure_unique_internal_transaction_data()

                    except Exception as e:
                        print(f"Error parsing PanelCustomerData_Internal data: {e}")
                        print(f"Fields: {fields}")
                
            elif topic == 'PanelPositionPNLSummary_Sim2':
                fields = data.strip().split(",")
                if len(fields) < 9:
                    print("PositionPNL verisi eksik.")
                    return
                symbol = fields[0].strip()
                try:
                    self.position_pnl_data[symbol] = {
                        "exposure": float(fields[1]),
                        "usd_equivalent": float(fields[2]),
                        "avg_cost_rate": float(fields[3]),
                        "unrealized_pnl": float(fields[4]),
                        "spread_pnl": float(fields[5]),
                        "matching_pnl": float(fields[6]),
                        "position_pnl": float(fields[7]),
                        "total_pnl": float(fields[8])
                    }
                except Exception as e:
                    print(f"Error parsing PositionPNL data: {e}")
                    print(f"Fields: {fields}")
            
            elif topic == 'PanelCustomerFlowSummary_Sim2':
                fields = data.strip().split(",")
                if len(fields) < 5:
                    print("CustomerFlow verisi eksik.")
                    return
                symbol = fields[0].strip()
                try:
                    self.customer_flow_data[symbol] = {
                        "total_amount": float(fields[1]),
                        "total_buy_amount": float(fields[2]),
                        "total_sell_amount": float(fields[3]),
                        "net_amount": float(fields[4])
                    }
                except Exception as e:
                    print(f"Error parsing CustomerFlow data: {e}")
                    print(f"Fields: {fields}")
            
            elif topic == 'PanelExecutionSummary_Sim2':
                fields = data.strip().split(",")
                if len(fields) < 5:
                    print("ExecutionSummary verisi eksik.")
                    return
                symbol = fields[0].strip()
                try:
                    self.execution_summary_data[symbol] = {
                        "total_amount": float(fields[1]),
                        "total_buy_amount": float(fields[2]),
                        "total_sell_amount": float(fields[3]),
                        "net_amount": float(fields[4])
                    }
                except Exception as e:
                    print(f"Error parsing ExecutionSummary data: {e}")
                    print(f"Fields: {fields}")
            
            elif topic == 'PanelCurrencyFlowSummary_Sim2':
                fields = data.strip().split(",")
                if len(fields) < 4:
                    print("CurrencyFlowSummary verisi eksik.")
                    return
                currency = fields[0].strip()
                try:
                    self.currency_flow_data[currency] = {
                        "total_buy_amount": float(fields[1]),
                        "total_sell_amount": float(fields[2]),
                        "net_amount": float(fields[3])
                    }
                except Exception as e:
                    print(f"Error parsing CurrencyFlowSummary data: {e}")
                    print(f"Fields: {fields}")
        except Exception as e:
            print(f"Error handling Kafka update: {e}")
            print(f"Data received: {data}")

    def handle_kafka_error(self, error_msg):
        """Handle Kafka errors"""
        print(f"Kafka error: {error_msg}")
        QMessageBox.warning(self, "Kafka Error", f"Connection error: {error_msg}")

#Update Tables

    def update_position_table(self):
        """Update position table from Kafka data"""
        live_keys = list(self.position_data.keys())
        DEFAULT_SYMBOL_ORDER=['USD/TRY','EUR/USD', 'XAU/USD','XAG/USD','XPT/USD','GBP/USD','USD/CHF','USD/SAR','USD/KWD','USD/JPY', 'USD/CAD', 'USD/SEK','USD/NOK','AUD/USD','NZD/USD', 'USD/AED', 'USD/BHD','USD/QAR', 'USD/OMR' ]
        want_blanks = True

        if want_blanks:
            symbols = DEFAULT_SYMBOL_ORDER[:]  # 19 satır sabit
        else:
            symbols = [s for s in DEFAULT_SYMBOL_ORDER if s in live_keys] + \
                    [s for s in live_keys if s not in DEFAULT_SYMBOL_ORDER]
        ##        print(f"Updating position table with {len(symbols)} symbols: {symbols}")
        
        # Temporarily disable sorting to prevent issues during update
        self.position_table.setSortingEnabled(False)
        t=self.position_table
        keep_key=self._get_selected_key(t)
        # Clear table first to prevent duplicates
        self.position_table.clearContents()
        self.position_table.setRowCount(len(symbols))
        
        for i, symbol in enumerate(symbols):
            data = self.position_data.get(symbol)
            if not data:
                continue

            # Symbol
            symbol_item = QTableWidgetItem(symbol)
            symbol_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter)
            #Key value
            key_value = f"{symbol}_{i}"
            symbol_item.setData(Qt.ItemDataRole.UserRole, key_value)
            self.position_table.setItem(i, 0, symbol_item)
            
            # Position
         
            position_item = NumberTableWidgetItem(f"{data['position']:,.0f}", data['position'])
            self.position_table.setItem(i, 1, position_item)
                        
            # Position Value

            pos_value = data['position_value']
            pos_value_item = CurrencyTableWidgetItem(pos_value,display_format="int")
            self.position_table.setItem(i, 2, pos_value_item)    

            # Avg Cost Rate
            avg_rate_item = NumberTableWidgetItem(f"{data['avg_cost_rate']:.4f}", data['avg_cost_rate'])
            self.position_table.setItem(i, 3, avg_rate_item)
            
            # Unrealized PNL
            unrealized_pnl = data['unrealized_pnl']
            unrealized_item = CurrencyTableWidgetItem(unrealized_pnl,display_format="int")
            unrealized_item.setForeground(QBrush(QColor(color_success if unrealized_pnl > 0 else "#FF7B63")))
            self.position_table.setItem(i, 4, unrealized_item)
            
            # Realized PNL
            realized_pnl = data['realized_pnl']
            realized_item = CurrencyTableWidgetItem(realized_pnl,display_format="int")
            realized_item.setForeground(QBrush(QColor(color_success if realized_pnl > 0 else "#FF7B63")))
            self.position_table.setItem(i, 5, realized_item)
            
            # Bid
            if symbol == 'XAU/USD' or symbol == 'XPT/USD':
                bid_item = NumberTableWidgetItem(f"{data['bid']:.2f}", data['bid'])
            else: 
                bid_item = NumberTableWidgetItem(f"{data['bid']:.4f}", data['bid'])
            self.position_table.setItem(i, 6, bid_item)

            # Ask
            if symbol == 'XAU/USD' or symbol == 'XPT/USD':
                ask_item = NumberTableWidgetItem(f"{data['ask']:.2f}", data['ask'])
            else: 
                ask_item = NumberTableWidgetItem(f"{data['ask']:.4f}", data['ask'])
            self.position_table.setItem(i, 7, ask_item)
            
            # Bid Venue
            bid_venue_item = QTableWidgetItem(data['bid_venue'])
            bid_venue_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter)
            self.position_table.setItem(i, 8, bid_venue_item)

            ##            self.position_table.setItem(i, 8, QTableWidgetItem(data['bid_venue']))
            
            # Ask Venue
            ask_venue_item = QTableWidgetItem(data['ask_venue'])
            ask_venue_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter)
            self.position_table.setItem(i, 9, ask_venue_item)

            ##            self.position_table.setItem(i, 9, QTableWidgetItem(data['ask_venue']))
            
            # Close Position Button
            close_button_container = self.create_close_position_button_widget(i, symbol)
            self.position_table.setCellWidget(i, 10, close_button_container)
        
        # Update cumulative metrics after updating position table
        self.update_cumulative_metrics()
        
        # Re-enable sorting after all data is populated
        self.position_table.setSortingEnabled(False)
        self.filter_table_by_text(self.position_table, self.position_search.text())
        self.position_table.setSortingEnabled(True)
        self._reselect_by_key(t,keep_key)
        # ✅ Uygulanan filtreler devam etsin
        if hasattr(self, "position_filter") and self.position_filter.active_filters:
            self.position_filter.apply_filters(keep_open=True)

    def update_execution_table(self):
        """Update execution table from stored execution data (show last 10)"""
        MAX_ROWS = 100
        # Use execution data list if available, otherwise show empty table
        executions = getattr(self, 'execution_data_list', [])
        if not executions:
            self.execution_table.clearContents()
            self.execution_table.setRowCount(0)
            return
        # Temporarily disable sorting to prevent issues during update
        self.execution_table.setSortingEnabled(False)
        # Ensure unique execution data
        # self.ensure_unique_execution_data()
        t=self.execution_table
        keep_key=self._get_selected_key(t)
        executions = self.execution_data_list
        # ---- Keep only the latest 10 ----
        has_ts = any('timestamp' in d and d.get('timestamp') for d in executions)
        if has_ts:
            try:
                # ISO timestamp ise string sıralama da çalışır; değilse parse et
                data_sorted = sorted(executions, key=lambda d: d.get('timestamp', ''), reverse=True)
                recent = data_sorted[:MAX_ROWS]          # en güncel -> ilk 10
            except Exception:
                recent = executions[-MAX_ROWS:]          # fallback: sondaki 10
        else:
            recent = executions[-MAX_ROWS:]              # zaman yoksa sona eklenmiştir varsay
        # Clear and size to recent only
        self.execution_table.clearContents()
        self.execution_table.setRowCount(len(recent))
        # ---- Populate table with RECENT, not 'executions' ----
        for i, execution in enumerate(recent):
            # Symbol
            symbol = execution.get('symbol', '')
            symbol_item = QTableWidgetItem(symbol)
            symbol_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter)
            
            #Key value
            key_value = f"{execution.get('symbol','')}_{execution.get('quantity','')}_{execution.get('timestamp','')}"
            symbol_item.setData(Qt.ItemDataRole.UserRole, key_value)
            self.execution_table.setItem(i, 0, symbol_item)
            # Quantity (sortable)
            quantity = execution.get('quantity', 0)
            quantity_text = f"{quantity:,}" if quantity >= 1000 else str(quantity)
            quantity_item = NumberTableWidgetItem(quantity_text, quantity)
            self.execution_table.setItem(i, 1, quantity_item)
            # Price (sortable)
            price = execution.get('price', 0.0)
            price_text = f"{price:.4f}" if price < 100 else f"{price:.2f}"
            price_item = NumberTableWidgetItem(price_text, price)
            self.execution_table.setItem(i, 2, price_item)
            # Side
            side = execution.get('side', '')
            side_item = QTableWidgetItem(side)
            side_item.setForeground(QBrush(QColor(color_success if side == "Buy" else "#FF7B63")))
            side_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter)
            self.execution_table.setItem(i, 3, side_item)
            # Venue
            venue_item = QTableWidgetItem(execution.get('venue', ''))
            venue_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter)
            self.execution_table.setItem(i, 4, venue_item)
            # Counterparty
            counterparty_item = QTableWidgetItem(execution.get('counterparty', ''))
            counterparty_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter)
            self.execution_table.setItem(i, 5, counterparty_item)
            # Timestamp
            ts_item = QTableWidgetItem(execution.get('timestamp', ''))
            ts_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter)
            self.execution_table.setItem(i, 6, ts_item)
            # Strategy
            strategy_item = QTableWidgetItem(execution.get('strategy', ''))
            strategy_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter)
            self.execution_table.setItem(i, 7, strategy_item)
            # Realized PnL (sortable)
            realized_pnl = execution.get('realizedPnL', 0)
            realized_item = CurrencyTableWidgetItem(realized_pnl, display_format="int")
            realized_item.setForeground(QBrush(QColor(color_success if realized_pnl > 0 else "#FF7B63")))
            self.execution_table.setItem(i, 8, realized_item)
        # Re-enable sorting after all data is populated
        
        # Re-enable sorting after all data is populated
        self.execution_table.setSortingEnabled(False)
        self.filter_table_by_text(self.execution_table, self.execution_search.text())
        self.execution_table.setSortingEnabled(True)
        self._reselect_by_key(t,keep_key)
        # ✅ Uygulanan filtreler devam etsin
        if hasattr(self, "execution_filter") and self.execution_filter.active_filters:
            self.execution_filter.apply_filters(keep_open=True)

    def update_strategy_table(self):
        """Update strategy table from Kafka data"""
        # Don't update GUI if we're waiting for Kafka confirmation or if we're in edit mode
        if self.waiting_for_strategy_kafka_confirmation:
            print("DEBUG: Skipping strategy table update - waiting for Kafka confirmation")
            return
            
        # Don't update if we're currently editing a row
        if self.editing_row is not None:
            print("DEBUG: Skipping strategy table update - currently in edit mode")
            return
            
        # Store current editing state before clearing
        current_editing_row = self.editing_row
        current_original_values = self.original_values.copy() if hasattr(self, 'original_values') else {}
            
        live_keys = list(self.strategy_data.keys())
        DEFAULT_SYMBOL_ORDER=['USD/TRY','EUR/USD', 'XAU/USD','XAG/USD','XPT/USD','GBP/USD','USD/CHF','USD/SAR','USD/KWD','USD/JPY', 'USD/CAD', 'USD/SEK','USD/NOK','AUD/USD','NZD/USD', 'USD/AED', 'USD/BHD','USD/QAR', 'USD/OMR' ]
        want_blanks = True
 
        if want_blanks:
            symbols = DEFAULT_SYMBOL_ORDER[:]  # 19 satır sabit
        else:
            symbols = [s for s in DEFAULT_SYMBOL_ORDER if s in live_keys] + \
                    [s for s in live_keys if s not in DEFAULT_SYMBOL_ORDER]
        
        # Temporarily disable sorting to prevent issues during update
        self.strategy_table.setSortingEnabled(False)
        t=self.strategy_table
        keep_key=self._get_selected_key(t)
        # Clear table first to prevent duplicates
        self.strategy_table.clearContents()
        self.strategy_table.setRowCount(len(symbols))
        
        for i, symbol in enumerate(symbols):
            if symbol not in self.strategy_data:
                continue
                
            data = self.strategy_data.get(symbol)
            
            # Create button widget for this row
            switch_container = self.create_strategy_switch_widget(i, symbol, data.get('status', 'Stopped'))
            #Key value

            self.strategy_table.setCellWidget(i, 0, switch_container)

            
            # Update symbol
            ##            self.strategy_table.setItem(i, 1, QTableWidgetItem(symbol))

            symbol_item = QTableWidgetItem(symbol)
            symbol_item.setFlags(symbol_item.flags() & ~Qt.ItemFlag.ItemIsEditable)  # Make non-editable
            symbol_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter)
            key_value = f"{data.get('symbol','')}_{data.get('transfer_amc','')}_{data.get('long_limit','')}_{i}"
            symbol_item.setData(Qt.ItemDataRole.UserRole, key_value)
            self.strategy_table.setItem(i, 1, symbol_item)
            
            # Update status
            status_string = data.get('status', 'Stopped')
            status_text = status_string.capitalize()  # Display as "Running" or "Stopped" in UI
            status_item = QTableWidgetItem(status_text)
            status_item.setFlags(status_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            if status_string.lower() == "running":
                status_item.setBackground(QBrush(QColor(color_success)))  # Green background for Running
            else:
                status_item.setBackground(QBrush(QColor("#FF7B63")))  # Red background for Stopped
            status_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter)
            self.strategy_table.setItem(i, 2, status_item)
            
            # Update other fields
            transfer_amc = data.get('transfer_amc', 0)
            transfer_item = NumberTableWidgetItem(f"{transfer_amc:,}", transfer_amc)
            self.strategy_table.setItem(i, 3, transfer_item)
            
            long_limit = data.get('long_limit', 0)
            long_limit_item = NumberTableWidgetItem(f"{long_limit:,}", long_limit)
            self.strategy_table.setItem(i, 4, long_limit_item)
            
            short_limit = data.get('short_limit', 0)
            short_limit_item = NumberTableWidgetItem(f"{short_limit:,}", short_limit)
            self.strategy_table.setItem(i, 5, short_limit_item)
            
            min_hedge = data.get('min_hedge', 0)
            min_hedge_item = NumberTableWidgetItem(f"{min_hedge:,}", min_hedge)
            self.strategy_table.setItem(i, 6, min_hedge_item)
            
            hedge_ratio = data.get('hedge_ratio', 0)
            ratio_item = NumberTableWidgetItem(f"{hedge_ratio:.0%}", hedge_ratio)
            self.strategy_table.setItem(i, 7, ratio_item)
            
            take_profit = data.get('take_profit', 0)
            take_profit_item = NumberTableWidgetItem(f"{take_profit}", take_profit)
            self.strategy_table.setItem(i, 8, take_profit_item)
            
            stop_loss = data.get('stop_loss', 0)
            stop_loss_item = NumberTableWidgetItem(f"{stop_loss}", stop_loss)
            self.strategy_table.setItem(i, 9, stop_loss_item)

            spread_check = data.get('spread_check', 0)
            spread_check_item = NumberTableWidgetItem(f"{spread_check:.1%}", spread_check)
            self.strategy_table.setItem(i, 10, spread_check_item)
            
            
            # Create edit button widget for this row
            edit_button_container = self.create_strategy_edit_button_widget(i, symbol) #edit butonu buradan oluşturuluyor.
            self.strategy_table.setCellWidget(i, 11, edit_button_container)
        
        # Restore edit mode highlighting if we were in edit mode
        if current_editing_row is not None and current_editing_row < len(symbols):
            # Find the symbol that was being edited
            original_symbol = None
            if current_editing_row < self.strategy_table.rowCount():
                original_item = self.strategy_table.item(current_editing_row, 1)
                if original_item:
                    original_symbol = original_item.text()
            
            # Find the new row for this symbol
            new_row = -1
            for i, symbol in enumerate(symbols):
                if symbol == original_symbol:
                    new_row = i
                    break
            
            if new_row != -1:
                # Restore edit mode for the new row
                self.editing_row = new_row
                self.original_values = current_original_values
                
                # Re-apply highlighting for editable columns only
                editable_columns = [3, 4, 5, 6, 7, 8, 9, 10]
                for col in range(self.strategy_table.columnCount()):
                    item = self.strategy_table.item(new_row, col)
                    if item:
                        if col in editable_columns:
                            # Highlight editable columns
                            item.setBackground(QBrush(QColor("#ff9900")))
                            item.setForeground(QBrush(QColor("#000000")))
                        else:
                            # Keep non-editable columns with normal appearance
                            item.setBackground(QBrush(QColor("#0A1929")))
                            item.setForeground(QBrush(QColor("#ffffff")))
                
                # Enable editing for the table
                self.strategy_table.setEditTriggers(QTableWidget.EditTrigger.DoubleClicked)
                
                # Replace edit button with Save/Cancel buttons
                button_container = QWidget()
                button_layout = QHBoxLayout(button_container)
                button_layout.setContentsMargins(2, 2, 2, 2)
                button_layout.setSpacing(2)
                
                save_btn = QPushButton("Save")
                save_btn.setFixedSize(40, 20)
                save_btn.setStyleSheet(f"""
                    QPushButton {{
                        background-color: {color_success} ; color: white; border: 1px solid #3FA86D;
                        padding: 2px; border-radius: 2px; font-size: 9px; font-weight: bold;
                    }}
                    QPushButton:hover {{ background-color: #3FA86D; }}
                """)
                save_btn.clicked.connect(lambda: self.save_strategy_edit(new_row))
                button_layout.addWidget(save_btn)
                
                cancel_btn = QPushButton("Cancel")
                cancel_btn.setFixedSize(40, 20)
                cancel_btn.setStyleSheet("""
                    QPushButton {
                        background-color: #FF7B63; color: white; border: 1px solid #DA6857;
                        padding: 2px; border-radius: 2px; font-size: 9px; font-weight: bold;
                    }
                    QPushButton:hover { background-color: #DA6857; }
                """)
                cancel_btn.clicked.connect(lambda: self.cancel_edit())
                button_layout.addWidget(cancel_btn)
                
                self.strategy_table.setCellWidget(new_row, 11, button_container)
        
        # Re-enable sorting after all data is populated
        self.filter_table_by_text(self.strategy_table, self.strategy_search.text())
        self.strategy_table.setSortingEnabled(True)
        self._reselect_by_key(t,keep_key)

    def update_risk_table(self):
        """Update risk table from Kafka data"""
        #print(f"update_risk_table() called - risk_data keys: {list(self.risk_data.keys())}")
        live_keys = list(self.position_data.keys())
        DEFAULT_SYMBOL_ORDER=['USD/TRY','EUR/USD', 'XAU/USD','XAG/USD','XPT/USD','GBP/USD','USD/CHF','USD/SAR','USD/KWD','USD/JPY', 'USD/CAD', 'USD/SEK','USD/NOK','AUD/USD','NZD/USD', 'USD/AED', 'USD/BHD','USD/QAR', 'USD/OMR' ]
        want_blanks = True
 
        if want_blanks:
            symbols = DEFAULT_SYMBOL_ORDER[:]  # 19 satır sabit
        else:
            symbols = [s for s in DEFAULT_SYMBOL_ORDER if s in live_keys] + \
                    [s for s in live_keys if s not in DEFAULT_SYMBOL_ORDER]
        ##        print(f"Updating position table with {len(symbols)} symbols: {symbols}")
        
        
        # Temporarily disable sorting to prevent issues during update
        self.risk_table.setSortingEnabled(False)

        t=self.risk_table
        keep_key=self._get_selected_key(t)
        # Clear table first to prevent duplicates
        self.risk_table.clearContents()
        self.risk_table.setRowCount(len(symbols))


        
        for i, symbol in enumerate(symbols):
            data = self.risk_data.get(symbol)
            if not data:
                continue
             # Symbol
            symbol_item = QTableWidgetItem(symbol)
            symbol_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter)
            # Key Value
            key_value =f"{symbol}_{i}"
            symbol_item.setData(Qt.ItemDataRole.UserRole, key_value)

            self.risk_table.setItem(i, 0, symbol_item)
            
            # KFH Feed Status
            status_item = QTableWidgetItem(data['KFH_feed_status'])
            if data['KFH_feed_status'] == "Healthy":
                status_item.setBackground(QBrush(QColor(color_success)))
            elif data['KFH_feed_status'] == "Warning":
                status_item.setBackground(QBrush(QColor("#ffaa00")))
            elif data['KFH_feed_status'] == "Disconnected":
                status_item.setBackground(QBrush(QColor("#FF7B63")))
            else:
                status_item.setBackground(QBrush(QColor("#90969E")))
            status_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter)
            self.risk_table.setItem(i, 1, status_item)

            # Integral Feed Status
            status_item = QTableWidgetItem(data['integral_feed_status'])
            if data['integral_feed_status'] == "Healthy":
                status_item.setBackground(QBrush(QColor(color_success)))
            elif data['integral_feed_status'] == "Warning":
                status_item.setBackground(QBrush(QColor("#ffaa00")))
            elif data['integral_feed_status'] == "Disconnected":
                status_item.setBackground(QBrush(QColor("#FF7B63")))
            else:
                status_item.setBackground(QBrush(QColor("#90969E")))
            status_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter)
            self.risk_table.setItem(i, 2, status_item)

            # Tradair Feed Status
            status_item = QTableWidgetItem(data['tradair_feed_status'])
            if data['tradair_feed_status'] == "Healthy":
                status_item.setBackground(QBrush(QColor(color_success)))
            elif data['tradair_feed_status'] == "Warning":
                status_item.setBackground(QBrush(QColor("#ffaa00")))
            elif data['tradair_feed_status'] == "Disconnected":
                status_item.setBackground(QBrush(QColor("#FF7B63")))
            else:
                status_item.setBackground(QBrush(QColor("#90969E")))
            status_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter)
            self.risk_table.setItem(i, 3, status_item)

            # 360T Feed Status
            status_item = QTableWidgetItem(data['360t_feed_status'])
            if data['360t_feed_status'] == "Healthy":
                status_item.setBackground(QBrush(QColor(color_success)))
            elif data['360t_feed_status'] == "Warning":
                status_item.setBackground(QBrush(QColor("#ffaa00")))
            elif data['360t_feed_status'] == "Disconnected":
                status_item.setBackground(QBrush(QColor("#FF7B63")))
            else:
                status_item.setBackground(QBrush(QColor("#90969E")))
            status_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter)
            self.risk_table.setItem(i, 4, status_item)

            # Strategy Status - use ONLY Kafka risk data (server sends "Running" or "Stopped")
            strategy_status = data.get('strategy_status', 'Stopped')
            
            strategy_item = QTableWidgetItem(str(strategy_status))
            if strategy_status == "Running":
                strategy_item.setBackground(QBrush(QColor(color_success)))
            else:
                strategy_item.setBackground(QBrush(QColor("#FF7B63")))
            strategy_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter)
            self.risk_table.setItem(i, 5, strategy_item)
            
            # Position Control
            pos_item = QTableWidgetItem(data['position_control'])
            if data['position_control'] == "OK":
                pos_item.setBackground(QBrush(QColor(color_success)))
            else:
                pos_item.setBackground(QBrush(QColor("#ffaa00")))
            pos_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter)
            self.risk_table.setItem(i, 6, pos_item)

                       
            # Last Order Status
            order_item = QTableWidgetItem(data['last_order_status'])
            if data['last_order_status'] == "Filled":
                order_item.setBackground(QBrush(QColor(color_success)))
            elif data['last_order_status'] == "Rejected":
                order_item.setBackground(QBrush(QColor("#FF7B63")))
            elif data['last_order_status'] == "Unknown":
                order_item.setBackground(QBrush(QColor("#FF7B63")))
            elif data['last_order_status'] == "Pending":
                order_item.setBackground(QBrush(QColor("#FF7B63")))
            elif data['last_order_status'] == "No Order":
                order_item.setBackground(QBrush(QColor(color_success)))
                ##            else:
                ##                order_item.setBackground(QBrush(QColor("#ffaa00")))
            order_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter)
            self.risk_table.setItem(i, 7, order_item)
            
            # Spread Check
            spread_item = QTableWidgetItem(data['spread_check'])
            if data['spread_check'] == "OK":
                spread_item.setBackground(QBrush(QColor(color_success)))  # Green for true
            else:
                spread_item.setBackground(QBrush(QColor("#FF7B63")))  # Red for false
            spread_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter)
            self.risk_table.setItem(i, 8, spread_item)
            
            # Position Flow Check
            flow_item = QTableWidgetItem(data['customer_flow_check'])
            if data['customer_flow_check'] == "Active":
                flow_item.setBackground(QBrush(QColor(color_success)))  # Green for true
            else:
                flow_item.setBackground(QBrush(QColor("#FF7B63")))  # Red for false
            flow_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter)
            self.risk_table.setItem(i, 9, flow_item)
        
        # Re-enable sorting after all data is populated
        self.filter_table_by_text(self.risk_table, self.risk_search.text())
        self.risk_table.setSortingEnabled(True)
        self._reselect_by_key(t,keep_key)
        # ✅ Uygulanan filtreler devam etsin
        if hasattr(self, "risk_filter") and self.risk_filter.active_filters:
            self.risk_filter.apply_filters(keep_open=True)
        
    def update_manual_trade_table(self):
        """Update execution table from stored execution data"""
        # Use execution data list if available, otherwise show empty table
        manual_trades = getattr(self, 'manual_trade_list', [])
        
        if not manual_trades:
            # If no execution data available, show empty table
            self.manual_trade_table.clearContents()
            self.manual_trade_table.setRowCount(0)
            return
        
        # Temporarily disable sorting to prevent issues during update
        self.manual_trade_table.setSortingEnabled(False)
        
        # Ensure unique execution data
        # self.ensure_unique_execution_data()
        t=self.manual_trade_table
        keep_key=self._get_selected_key(t)
        manual_trades = self.manual_trade_list
        
        # Clear table first to prevent duplicates
        self.manual_trade_table.clearContents()
        self.manual_trade_table.setRowCount(len(manual_trades))
        
        for i, manual_trade in enumerate(manual_trades):
            # Symbol
            symbol = manual_trade.get('symbol', '')
            symbol_item = QTableWidgetItem(symbol)
            symbol_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter)
            #Key value
            key_value = f"{manual_trade.get('symbol','')}_{manual_trade.get('timestamp','')}"
            symbol_item.setData(Qt.ItemDataRole.UserRole, key_value)
            self.manual_trade_table.setItem(i, 0, symbol_item)
        
            # Quantity with proper sorting
            quantity = manual_trade.get('quantity', 0)
            quantity_text = f"{quantity:,}" if quantity >= 1000 else str(quantity)
            quantity_item = NumberTableWidgetItem(quantity_text, quantity)
            self.manual_trade_table.setItem(i, 1, quantity_item)
            
            # Price with proper sorting
            price = manual_trade.get('price', 0.0)
            price_text = f"{price:.4f}" if price < 100 else f"{price:.2f}"
            price_item = NumberTableWidgetItem(price_text, price)
            self.manual_trade_table.setItem(i, 2, price_item)
            
            # Side with color coding
            side = manual_trade.get('side', '')
            side_item = QTableWidgetItem(side)
            if side == "Buy":
                side_item.setForeground(QBrush(QColor(color_success )))
            else:
                side_item.setForeground(QBrush(QColor("#FF7B63")))
            side_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter)
            self.manual_trade_table.setItem(i, 3, side_item)

            # Remaining columns
            venue = manual_trade.get('venue', '')
            venue_item = QTableWidgetItem(venue)
            venue_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter)
            self.manual_trade_table.setItem(i, 4, venue_item)

            counterparty = manual_trade.get('counterparty', '')
            counterparty_item = QTableWidgetItem(counterparty)
            counterparty_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter)
            self.manual_trade_table.setItem(i, 5, counterparty_item)

            timestamp = manual_trade.get('timestamp', '')
            timestamp_item = QTableWidgetItem(timestamp)
            timestamp_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter)
            self.manual_trade_table.setItem(i, 6, timestamp_item)

            strategy = manual_trade.get('strategy', '')
            strategy_item = QTableWidgetItem(strategy)
            strategy_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter)
            self.manual_trade_table.setItem(i, 7, strategy_item)
                        

            realized_pnl = manual_trade.get('realizedPnL', 0)
            realized_item = CurrencyTableWidgetItem(realized_pnl,display_format="int")
            realized_item.setForeground(QBrush(QColor(color_success if realized_pnl > 0 else "#FF7B63")))
            self.manual_trade_table.setItem(i, 8, realized_item)

        # Re-enable sorting after all data is populated
        
        self.filter_table_by_text(self.manual_trade_table, self.manual_trade_search.text())
        self.manual_trade_table.setSortingEnabled(True)
        self._reselect_by_key(t,keep_key)

    def update_provider_table(self):
        """Update provider table from Kafka data"""
        # Don't update GUI if we're waiting for Kafka confirmation or if we're in edit mode
        live_keys = list(self.provider_data.keys())
        

        DEFAULT_SYMBOL_ORDER=['USD/TRY','EUR/USD', 'XAU/USD','XAG/USD','XPT/USD','GBP/USD','USD/CHF','USD/SAR','USD/KWD','USD/JPY', 'USD/CAD', 'USD/SEK','USD/NOK','AUD/USD','NZD/USD', 'USD/AED', 'USD/BHD','USD/QAR', 'USD/OMR' ]
        want_blanks = True

        if want_blanks:
            symbols = DEFAULT_SYMBOL_ORDER[:]  # 19 satır sabit
        else:
            symbols = [s for s in DEFAULT_SYMBOL_ORDER if s in live_keys] + \
                    [s for s in live_keys if s not in DEFAULT_SYMBOL_ORDER]
        ##        print(f"Updating position table with {len(symbols)} symbols: {symbols}")
        
        # Temporarily disable sorting to prevent issues during update
        self.provider_table.setSortingEnabled(False)
        t=self.provider_table
        keep_key=self._get_selected_key(t)
        if self.waiting_for_provider_kafka_confirmation:
            print("DEBUG: Skipping provider table update - waiting for Kafka confirmation")
            return
                        
        # symbols = list(self.provider_data.keys())
        # Temporarily disable sorting to prevent issues during update
        self.provider_table.setSortingEnabled(False)
        
        # Clear table first to prevent duplicates
        self.provider_table.clearContents()
        self.provider_table.setRowCount(len(symbols))
        
        for i, symbol in enumerate(symbols):
            if symbol not in self.provider_data:
                continue

            data = self.provider_data[symbol]
            # a=self.provider_data
            # print(self.provider_data)
            # print(self.provider_data)
             # Update symbol
            ##            self.provider_table.setItem(i, 1, QTableWidgetItem(symbol))

            symbol_item = QTableWidgetItem(symbol)
            symbol_item.setFlags(symbol_item.flags() & ~Qt.ItemFlag.ItemIsEditable)  # Make non-editable
            symbol_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter)
            self.provider_table.setItem(i, 0, symbol_item)            
           
            # Create button widget for this row
            switch_container = self.create_provider_switch_widget(i,1, symbol, data.get('kfh_gui_status', 'True'))
            self.provider_table.setCellWidget(i, 1, switch_container)

            switch_container = self.create_provider_switch_widget(i,2, symbol, data.get('integral_gui_status', 'True'))
            self.provider_table.setCellWidget(i, 2, switch_container)

            switch_container = self.create_provider_switch_widget(i,3, symbol, data.get('tradair_gui_status', 'True'))
            self.provider_table.setCellWidget(i, 3, switch_container)

            switch_container = self.create_provider_switch_widget(i,4, symbol, data.get('360t_gui_status', 'True'))
            self.provider_table.setCellWidget(i, 4, switch_container)
            
             
        # Re-enable sorting after all data is populated
        self.filter_table_by_text(self.provider_table, self.provider_search.text())
        # self.provider_table.setSortingEnabled(True)
        self._reselect_by_key(t,keep_key)

    def update_customer_transaction_table(self):
        """Update customer transaction table from stored customer transaction data
            Shows only the most recent 50 rows on the table."""
        MAX_ROWS = 100
        # Use customer transaction data list if available, otherwise show empty table
        customer_transactions = getattr(self, 'customer_transaction_data_list', [])
        if not customer_transactions:
            # If no customer transaction data available, show empty table
            self.customer_transaction_table.clearContents()
            self.customer_transaction_table.setRowCount(0)
            return
        # Temporarily disable sorting to prevent issues during update
        self.customer_transaction_table.setSortingEnabled(False)
        # Ensure unique customer transaction data (your existing de-dup logic)
        # self.ensure_unique_execution_data()
        t=self.customer_transaction_table
        keep_key=self._get_selected_key(t)
        data = self.customer_transaction_data_list
        # ---- Keep only the latest 50 ----
        # If timestamps exist (ISO-like), sort by timestamp desc and take first 50
        has_ts = any('timestamp' in d and d.get('timestamp') for d in data)
        if has_ts:
            try:
                data_sorted = sorted(data, key=lambda d: d.get('timestamp', ''), reverse=True)
                recent = data_sorted[:MAX_ROWS]
            except Exception:
                # Fallback: if something goes wrong with sorting, take tail
                recent = data[-MAX_ROWS:]
        else:
            # No timestamp -> assume append order; take tail
            recent = data[-MAX_ROWS:]
        # Clear table first to prevent duplicates
        self.customer_transaction_table.clearContents()
        self.customer_transaction_table.setRowCount(len(recent))
        for i, customer_transaction in enumerate(recent):
            # Symbol
            symbol = customer_transaction.get('symbol', '')
            symbol_item = QTableWidgetItem(symbol)
            symbol_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter)
            #Key value
            key_value = f"{customer_transaction.get('symbol','')}_{customer_transaction.get('tranid','')}"
            symbol_item.setData(Qt.ItemDataRole.UserRole, key_value)
            self.customer_transaction_table.setItem(i, 0, symbol_item)
            # Quantity with proper sorting
            quantity = customer_transaction.get('quantity', 0)
            quantity_text = f"{quantity:,}" if quantity >= 1000 else str(quantity)
            quantity_item = NumberTableWidgetItem(quantity_text, quantity)
            self.customer_transaction_table.setItem(i, 1, quantity_item)
            # Side with color coding
            side = customer_transaction.get('side', '')
            side_item = QTableWidgetItem(side)
            if side == "CustomerBuy":
                side_item.setForeground(QBrush(QColor(color_success)))
            else:
                side_item.setForeground(QBrush(QColor("#FF7B63")))
            side_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter)
            self.customer_transaction_table.setItem(i, 2, side_item)
            # Tran Price with proper sorting
            price = customer_transaction.get('tranprice', 0.0)
            price_text = f"{price:.4f}" if price < 100 else f"{price:.2f}"
            price_item = NumberTableWidgetItem(price_text, price)
            self.customer_transaction_table.setItem(i, 3, price_item)
            # Timestamp
            timestamp = customer_transaction.get('timestamp', '')
            timestamp_item = QTableWidgetItem(timestamp)
            timestamp_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter)
            self.customer_transaction_table.setItem(i, 4, timestamp_item)
            # Customer ID
            customerid = customer_transaction.get('customerid', '')
            customerid_item = QTableWidgetItem(customerid)
            customerid_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter)
            self.customer_transaction_table.setItem(i, 5, customerid_item)
            # Bid Price
            bidprice = customer_transaction.get('bidprice', 0.0)
            bidprice_text = f"{bidprice:.4f}" if bidprice < 100 else f"{bidprice:.2f}"
            bidprice_item = NumberTableWidgetItem(bidprice_text, bidprice)
            self.customer_transaction_table.setItem(i, 6, bidprice_item)
            # Ask Price
            askprice = customer_transaction.get('askprice', 0.0)
            askprice_text = f"{askprice:.4f}" if askprice < 100 else f"{askprice:.2f}"
            askprice_item = NumberTableWidgetItem(askprice_text, askprice)
            self.customer_transaction_table.setItem(i, 7, askprice_item)
            # Salespnl with proper sorting
            salespnl = customer_transaction.get('salespnl', 0.0)
            salespnl_text = salespnl  # if salespnl < 100 else f"{salespnl:.2f}"
            salespnl_item = NumberTableWidgetItem(salespnl_text, salespnl)
            self.customer_transaction_table.setItem(i, 8, salespnl_item)
            #Tran ID
            tranid = customer_transaction.get('tranid', '')
            tranid_item = QTableWidgetItem(tranid)
            tranid_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter)
            self.customer_transaction_table.setItem(i, 9, tranid_item)
        # Re-enable sorting after all data is populated
        self.filter_table_by_text(self.customer_transaction_table, self.customer_transaction_search.text())
        self.customer_transaction_table.setSortingEnabled(True)
        self._reselect_by_key(t,keep_key)
        # ✅ Uygulanan filtreler devam etsin
        if hasattr(self, "customer_transaction_filter") and self.customer_transaction_filter.active_filters:
            self.customer_transaction_filter.apply_filters(keep_open=True)

    def update_internal_transaction_table(self):
        """Update internal transaction table from stored customer transaction data
            Shows only the most recent 50 rows on the table."""
        MAX_ROWS = 100
        # Use internal transaction data list if available, otherwise show empty table
        internal_transactions = getattr(self, 'internal_transaction_data_list', [])
        if not internal_transactions:
            # If no internal transaction data available, show empty table
            self.internal_transaction_table.clearContents()
            self.internal_transaction_table.setRowCount(0)
            return
        # Temporarily disable sorting to prevent issues during update
        self.internal_transaction_table.setSortingEnabled(False)
        # Ensure unique internal transaction data (your existing de-dup logic)
        # self.ensure_unique_execution_data()
        t=self.internal_transaction_table
        keep_key=self._get_selected_key(t)
        data = self.internal_transaction_data_list
        # ---- Keep only the latest 50 ----
        # If timestamps exist (ISO-like), sort by timestamp desc and take first 50
        has_ts = any('timestamp' in d and d.get('timestamp') for d in data)
        if has_ts:
            try:
                data_sorted = sorted(data, key=lambda d: d.get('timestamp', ''), reverse=True)
                recent = data_sorted[:MAX_ROWS]
            except Exception:
                # Fallback: if something goes wrong with sorting, take tail
                recent = data[-MAX_ROWS:]
        else:
            # No timestamp -> assume append order; take tail
            recent = data[-MAX_ROWS:]
        # Clear table first to prevent duplicates
        self.internal_transaction_table.clearContents()
        self.internal_transaction_table.setRowCount(len(recent))
        for i, internal_transaction in enumerate(recent):
            # Symbol
            symbol = internal_transaction.get('symbol', '')
            symbol_item = QTableWidgetItem(symbol)
            symbol_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter)
            #Key value
            key_value = f"{internal_transaction.get('symbol','')}_{internal_transaction.get('tranid','')}"
            symbol_item.setData(Qt.ItemDataRole.UserRole, key_value)
            self.internal_transaction_table.setItem(i, 0, symbol_item)
            # Quantity with proper sorting
            quantity = internal_transaction.get('quantity', 0)
            quantity_text = f"{quantity:,}" if quantity >= 1000 else str(quantity)
            quantity_item = NumberTableWidgetItem(quantity_text, quantity)
            self.internal_transaction_table.setItem(i, 1, quantity_item)
            # Side with color coding
            side = internal_transaction.get('side', '')
            side_item = QTableWidgetItem(side)
            if side == "CustomerBuy":
                side_item.setForeground(QBrush(QColor(color_success)))
            else:
                side_item.setForeground(QBrush(QColor("#FF7B63")))
            side_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter)
            self.internal_transaction_table.setItem(i, 2, side_item)
            # Tran Price with proper sorting
            price = internal_transaction.get('tranprice', 0.0)
            price_text = f"{price:.4f}" if price < 100 else f"{price:.2f}"
            price_item = NumberTableWidgetItem(price_text, price)
            self.internal_transaction_table.setItem(i, 3, price_item)
            # Timestamp
            timestamp = internal_transaction.get('timestamp', '')
            timestamp_item = QTableWidgetItem(timestamp)
            timestamp_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter)
            self.internal_transaction_table.setItem(i, 4, timestamp_item)
            # Customer ID
            entity = internal_transaction.get('entity', '')
            entity_item = QTableWidgetItem(entity)
            entity_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter)
            self.internal_transaction_table.setItem(i, 5, entity_item)

        # Re-enable sorting after all data is populated
        self.filter_table_by_text(self.internal_transaction_table, self.internal_transaction_search.text())
        self.internal_transaction_table.setSortingEnabled(True)
        self._reselect_by_key(t,keep_key)
        # ✅ Uygulanan filtreler devam etsin
        if hasattr(self, "internal_transaction_filter") and self.internal_transaction_filter.active_filters:
            self.internal_transaction_filter.apply_filters(keep_open=True)

    def update_position_pnl_table(self):
        """Populate Position & PNL table with Kafka data"""
        # Use Kafka data directly
        symbols = list(self.position_pnl_data.keys())


        
        if not symbols:
            self.position_pnl_table.clearContents()
            self.position_pnl_table.setRowCount(0)
            return
        
        # Temporarily disable sorting to prevent issues during update
        self.position_pnl_table.setSortingEnabled(False)
        t=self.position_pnl_table
        keep_key=self._get_selected_key(t)
        # Clear table first to prevent duplicates
        self.position_pnl_table.clearContents()
        self.position_pnl_table.setRowCount(len(symbols))
        
        for i, symbol in enumerate(symbols):
            pnl_data = self.position_pnl_data[symbol]
            
            # Symbol
            symbol_item = QTableWidgetItem(symbol)
            symbol_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter)
            #Key value
            key_value = f"{pnl_data.get('currency','')}_{pnl_data.get('exposure','')}_{pnl_data.get('avg_cost_rate','')}"
            symbol_item.setData(Qt.ItemDataRole.UserRole, key_value)
            self.position_pnl_table.setItem(i, 0, symbol_item)
            
            # Exposure
            exposure_item = NumberTableWidgetItem(f"{pnl_data.get('exposure', 0):,.0f}", pnl_data.get('exposure', 0))
            self.position_pnl_table.setItem(i, 1, exposure_item)


            # USD Equivalent
            usd_equivalent = pnl_data.get('usd_equivalent', 0)
            usd_equivalent_item = CurrencyTableWidgetItem(usd_equivalent, "int")
            self.position_pnl_table.setItem(i, 2, usd_equivalent_item)
            
            # Avg Cost Rate
            avg_cost_rate_item = NumberTableWidgetItem(f"{pnl_data.get('avg_cost_rate', 0):,.4f}", pnl_data.get('avg_cost_rate', 0))
            self.position_pnl_table.setItem(i, 3, avg_cost_rate_item)
          
            # Unrealized PNL
            unrealized_pnl = pnl_data.get('unrealized_pnl', 0)
            unrealized_pnl_item = CurrencyTableWidgetItem(unrealized_pnl, "int")
            unrealized_pnl_item.setForeground(QBrush(QColor(color_success if unrealized_pnl > 0 else "#FF7B63")))
            self.position_pnl_table.setItem(i, 4, unrealized_pnl_item)
            
            # Spread PNL
            spread_pnl = pnl_data.get('spread_pnl', 0)
            spread_pnl_item = CurrencyTableWidgetItem(spread_pnl, "int")
            spread_pnl_item.setForeground(QBrush(QColor(color_success if spread_pnl > 0 else "#FF7B63")))
            self.position_pnl_table.setItem(i, 5, spread_pnl_item)
            
            # Matching PNL
            matching_pnl = pnl_data.get('matching_pnl', 0)
            matching_pnl_item = CurrencyTableWidgetItem(matching_pnl, "int")
            matching_pnl_item.setForeground(QBrush(QColor(color_success if matching_pnl > 0 else "#FF7B63")))
            self.position_pnl_table.setItem(i, 6, matching_pnl_item)
            
            # Position PNL
            position_pnl = pnl_data.get('position_pnl', 0)
            position_pnl_item = CurrencyTableWidgetItem(position_pnl, "int")
            position_pnl_item.setForeground(QBrush(QColor(color_success if position_pnl > 0 else "#FF7B63")))
            self.position_pnl_table.setItem(i, 7, position_pnl_item)
            
            # Total PNL
            total_pnl = pnl_data.get('total_pnl', 0)
            total_pnl_item = CurrencyTableWidgetItem(total_pnl, "int")
            total_pnl_item.setForeground(QBrush(QColor(color_success if total_pnl > 0 else "#FF7B63")))
            self.position_pnl_table.setItem(i, 8, total_pnl_item)
        
        # Re-enable sorting after all data is populated
        self.filter_table_by_text(self.position_pnl_table, self.position_pnl_search.text())
        self.position_pnl_table.setSortingEnabled(True)
        self._reselect_by_key(t,keep_key)

    def update_customer_flow_table(self):
        """Populate Position Flow Summary table with Kafka data"""
        # Use Kafka data directly
        symbols = list(self.customer_flow_data.keys())
        if not symbols:
            self.customer_flow_table.clearContents()
            self.customer_flow_table.setRowCount(0)
            return
        
        # Temporarily disable sorting to prevent issues during update
        self.customer_flow_table.setSortingEnabled(False)
        t=self.customer_flow_table
        keep_key=self._get_selected_key(t)
        
        # Clear table first to prevent duplicates
        self.customer_flow_table.clearContents()
        self.customer_flow_table.setRowCount(len(symbols))
        
        for i, symbol in enumerate(symbols):
            flow_data = self.customer_flow_data[symbol]
            
            # Symbol
            symbol_item = QTableWidgetItem(symbol)
            symbol_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter)
            #Key value
            key_value = f"{flow_data.get('symbol','')}_{flow_data.get('total_amount','')}_{flow_data.get('total_buy_amount','')}"
            symbol_item.setData(Qt.ItemDataRole.UserRole, key_value)
            self.customer_flow_table.setItem(i, 0, symbol_item)
            
            # Total Amount
            total_amount_item = NumberTableWidgetItem(f"{flow_data.get('total_amount', 0):,.0f}", flow_data.get('total_amount', 0))
            self.customer_flow_table.setItem(i, 1, total_amount_item)

            
            # Total Buy Amount
            total_buy_item = NumberTableWidgetItem(f"{flow_data.get('total_buy_amount', 0):,.0f}", flow_data.get('total_buy_amount', 0))
            total_buy_item.setForeground(QBrush(QColor(color_success )))
            self.customer_flow_table.setItem(i, 2, total_buy_item)

            
            # Total Sell Amount
            total_sell_item = NumberTableWidgetItem(f"{flow_data.get('total_sell_amount', 0):,.0f}", flow_data.get('total_sell_amount', 0))
            total_sell_item.setForeground(QBrush(QColor("#FF7B63")))
            self.customer_flow_table.setItem(i, 3, total_sell_item)
            
            # Net Amount
            net_amount = flow_data.get('net_amount', 0)
            net_amount_item = NumberTableWidgetItem(f"{flow_data.get('net_amount', 0):,.0f}", flow_data.get('net_amount', 0))
            net_amount_item.setForeground(QBrush(QColor(color_success if net_amount > 0 else "#FF7B63")))
            self.customer_flow_table.setItem(i, 4, net_amount_item)


        
        # Re-enable sorting after all data is populated
        self.filter_table_by_text(self.customer_flow_table, self.customer_flow_search.text())
        self.customer_flow_table.setSortingEnabled(True)
        self._reselect_by_key(t,keep_key)

    def update_execution_summary_table(self):
        """Populate Execution Summary table with Kafka data"""
        # Use Kafka data directly
        symbols = list(self.execution_summary_data.keys())
        if not symbols:
            self.execution_summary_table.clearContents()
            self.execution_summary_table.setRowCount(0)
            return
        
        # Temporarily disable sorting to prevent issues during update
        self.execution_summary_table.setSortingEnabled(False)
        t=self.execution_summary_table
        keep_key=self._get_selected_key(t)
        
        # Clear table first to prevent duplicates
        self.execution_summary_table.clearContents()
        self.execution_summary_table.setRowCount(len(symbols))
        
        for i, symbol in enumerate(symbols):
            summary = self.execution_summary_data[symbol]
            
            # Symbol
            symbol_item = QTableWidgetItem(symbol)
            symbol_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter)
            #Key value
            key_value = f"{summary.get('symbol','')}_{summary.get('total_amount','')}_{summary.get('total_buy_amount','')}"
            symbol_item.setData(Qt.ItemDataRole.UserRole, key_value)
            self.execution_summary_table.setItem(i, 0, symbol_item)
            
            # Total Amount
            total_amount_item = NumberTableWidgetItem(f"{summary.get('total_amount', 0):,.0f}", summary.get('total_amount', 0))
            self.execution_summary_table.setItem(i, 1, total_amount_item)

            # Total Buy Amount
            total_buy_item = NumberTableWidgetItem(f"{summary.get('total_buy_amount', 0):,.0f}", summary.get('total_buy_amount', 0))
            total_buy_item.setForeground(QBrush(QColor(color_success )))
            self.execution_summary_table.setItem(i, 2, total_buy_item)
           
            # Total Sell Amount
            total_sell_item = NumberTableWidgetItem(f"{summary.get('total_sell_amount', 0):,.0f}", summary.get('total_sell_amount', 0))
            total_sell_item.setForeground(QBrush(QColor("#FF7B63")))
            self.execution_summary_table.setItem(i, 3, total_sell_item)          
           
            # Net Amount
            net_amount = summary.get('net_amount', 0)
            net_amount_item = NumberTableWidgetItem(f"{summary.get('net_amount', 0):,.0f}", summary.get('net_amount', 0))
            net_amount_item.setForeground(QBrush(QColor(color_success if net_amount > 0 else "#FF7B63")))
            self.execution_summary_table.setItem(i, 4, net_amount_item)

 
          
        # Re-enable sorting after all data is populated
        self.filter_table_by_text(self.execution_summary_table, self.execution_summary_search.text())
        self.execution_summary_table.setSortingEnabled(True)
        self._reselect_by_key(t,keep_key)

    def update_currency_flow_table(self):
        """Populate Currency Flow Summary table with Kafka data"""
        # Use Kafka data directly
        currencies = list(self.currency_flow_data.keys())
        if not currencies:
            self.currency_flow_table.clearContents()
            self.currency_flow_table.setRowCount(0)
            return
        
        # Temporarily disable sorting to prevent issues during update
        self.currency_flow_table.setSortingEnabled(False)
        t=self.currency_flow_table
        keep_key=self._get_selected_key(t)
        # Clear table first to prevent duplicates
        self.currency_flow_table.clearContents()
        self.currency_flow_table.setRowCount(len(currencies))
        
        for i, currency in enumerate(currencies):
            flow_data = self.currency_flow_data[currency]
            
            # Currency
            currency_item = QTableWidgetItem(currency)
            currency_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter)
            #Key value
            key_value = f"{flow_data.get('symbol','')}_{flow_data.get('total_buy_amount','')}_{flow_data.get('total_sell_amount','')}"
            currency_item.setData(Qt.ItemDataRole.UserRole, key_value)
            self.currency_flow_table.setItem(i, 0, currency_item)
            
            # Total Buy Amount
            total_buy_item = NumberTableWidgetItem(f"{flow_data.get('total_buy_amount', 0):,.0f}", flow_data.get('total_buy_amount', 0))
            total_buy_item.setForeground(QBrush(QColor(color_success )))
            self.currency_flow_table.setItem(i, 1, total_buy_item)
           
            # Total Sell Amount
            total_sell_item = NumberTableWidgetItem(f"{flow_data.get('total_sell_amount', 0):,.0f}", flow_data.get('total_sell_amount', 0))
            total_sell_item.setForeground(QBrush(QColor("#FF7B63")))
            self.currency_flow_table.setItem(i, 2, total_sell_item)          
           
            # Net Amount
            net_amount = flow_data.get('net_amount', 0)
            net_item = NumberTableWidgetItem(f"{flow_data.get('net_amount', 0):,.0f}", flow_data.get('net_amount', 0))
            net_item.setForeground(QBrush(QColor(color_success if net_amount > 0 else "#FF7B63")))
            self.currency_flow_table.setItem(i, 3, net_item)

        
        # Re-enable sorting after all data is populated
        self.filter_table_by_text(self.currency_flow_table, self.currency_flow_search.text())
        self.currency_flow_table.setSortingEnabled(True)
        self._reselect_by_key(t,keep_key)


    def filter_table_by_text(self, table, text: str):
        t = (text or "").lower()
        for r in range(table.rowCount()):
            visible = False
            for c in range(table.columnCount()):
                it = table.item(r, c)
                if it and t in it.text().lower():
                    visible = True
                    break
            table.setRowHidden(r, not visible)

    def create_strategy_edit_button_widget(self, row, symbol):
        """Create an edit button widget for strategy row"""
        edit_btn = QPushButton("Edit")
        edit_btn.setFixedSize(60, 20)
        edit_btn.setStyleSheet(f"""
            QPushButton {{
                background-color: {color_theme}; color: white; border: 1px solid {color_theme};
                padding: 2px 4px; border-radius: 2px; font-size: 10px; font-weight: bold;
            }}
            QPushButton:hover {{ background-color: {color_theme_hover}; }}
            QPushButton:pressed {{ background-color: {color_theme_pressed}; }}
        """)
        # Connect with symbol to find correct row dynamically
        edit_btn.clicked.connect(lambda checked, symbol=symbol: self.edit_strategy_row_by_symbol(symbol))
        
        # Create container widget to center the button
        button_container = QWidget()
        button_layout = QHBoxLayout(button_container)
        button_layout.setContentsMargins(0, 0, 0, 0)
        button_layout.addStretch()
        button_layout.addWidget(edit_btn)
        button_layout.addStretch()
        
        return button_container


    def edit_strategy_row_by_symbol(self, symbol):
        """Enter edit mode for a strategy row by finding the symbol"""
        # Find the row that contains this symbol
        row = -1
        for i in range(self.strategy_table.rowCount()):
            if self.strategy_table.item(i, 1) and self.strategy_table.item(i, 1).text() == symbol:
                row = i
                break
        
        if row == -1:
            print(f"Symbol {symbol} not found in table")
            return
            
        self.edit_strategy_row(row)

    def edit_strategy_row(self, row):
        """Enter edit mode for a strategy row"""
        if self.editing_row is not None and self.editing_row != row:
            # Cancel previous edit
            self.cancel_edit()
        
        self.editing_row = row
        symbol = self.strategy_table.item(row, 1).text()
        
        # Store original values
        self.original_values[row] = {}
        editable_columns = [3, 4, 5, 6, 7, 8, 9, 10]  # Editable columns
        for col in editable_columns:
            item = self.strategy_table.item(row, col)
            if item:
                self.original_values[row][col] = item.text()
        
        # Enable editing for the table
        self.strategy_table.setEditTriggers(QTableWidget.EditTrigger.DoubleClicked)
        
        # Highlight only editable columns with theme color
        for col in range(self.strategy_table.columnCount()):
            item = self.strategy_table.item(row, col)
            if item:
                if col in editable_columns:
                    # Highlight editable columns
                    item.setBackground(QBrush(QColor("#ff9900")))
                    item.setForeground(QBrush(QColor("#000000")))
                else:
                    # Keep non-editable columns with normal appearance
                    item.setBackground(QBrush(QColor("#0A1929")))
                    item.setForeground(QBrush(QColor("#ffffff")))
        
        # Replace Edit button with Save/Cancel buttons
        button_container = QWidget()
        button_layout = QHBoxLayout(button_container)
        button_layout.setContentsMargins(2, 2, 2, 2)
        button_layout.setSpacing(2)
        
        save_btn = QPushButton("Save")
        save_btn.setFixedSize(40, 20)
        save_btn.setStyleSheet(f"""
            QPushButton {{
                background-color: {color_success} ; color: white; border: 1px solid #3FA86D;
                padding: 2px; border-radius: 2px; font-size: 9px; font-weight: bold;
            }}
            QPushButton:hover {{ background-color: #3FA86D; }}
        """)
        save_btn.clicked.connect(lambda: self.save_strategy_edit(row))
        button_layout.addWidget(save_btn)
        
        cancel_btn = QPushButton("Cancel")
        cancel_btn.setFixedSize(40, 20)
        cancel_btn.setStyleSheet("""
            QPushButton {
                background-color: #FF7B63; color: white; border: 1px solid #DA6857;
                padding: 2px; border-radius: 2px; font-size: 9px; font-weight: bold;
            }
            QPushButton:hover { background-color: #DA6857; }
        """)
        cancel_btn.clicked.connect(lambda: self.cancel_edit())
        button_layout.addWidget(cancel_btn)
        
        self.strategy_table.setCellWidget(row, 11, button_container)
        
        print(f"Entered edit mode for row {row} ({symbol})")
    
    def save_strategy_edit(self, row):
        """Save the edited strategy row"""
        try:
            symbol = self.strategy_table.item(row, 1).text()
            
            # Get current status from the table
            status_item = self.strategy_table.item(row, 2)
            current_status = status_item.text() if status_item else "Stopped"
            
        
            transfer_amc = float(self.strategy_table.item(row, 3).text().replace(',', ''))
            long_limit = float(self.strategy_table.item(row, 4).text().replace(',', ''))
            short_limit = float(self.strategy_table.item(row, 5).text().replace(',', ''))
            min_hedge = float(self.strategy_table.item(row, 6).text().replace(',', ''))
            hedge_ratio = float(self.strategy_table.item(row, 7).text().replace('%', '').replace(',', ''))
            take_profit = float(self.strategy_table.item(row, 8).text().replace(',', ''))
            stop_loss = float(self.strategy_table.item(row, 9).text().replace(',', ''))
            print(self.strategy_table.item(row, 10).text())
            spread_check = float(self.strategy_table.item(row, 10).text().replace('%', '').replace(',', ''))

            # Validate Transfer Amount
            if transfer_amc < 0:
                QMessageBox.warning(self, "Invalid Input", "Transfer Amount cannot be negative !")
                return
            # Validate Long Position Limit
            if long_limit <= 0:
                QMessageBox.warning(self, "Invalid Input", "Long Position Limit must be greater than zero !")
                return
            # Validate Short Position Limit
            if short_limit >= 0:
                QMessageBox.warning(self, "Invalid Input", "Short Position Limit must be lower than zero !")
                return
            # Validate relationship between Long and Short limits
            if short_limit > long_limit:
                QMessageBox.warning(self, "Invalid Input", "Short Position Limit cannot exceed Long Position Limit !")
                return
            # Validate Min Hedge
            if min_hedge < 0:
                QMessageBox.warning(self, "Invalid Input", "Min Hedge cannot be negative !")
                return
            # Validate Min Hedge
            if min_hedge > long_limit:
                QMessageBox.warning(self, "Invalid Input", "Min Hedge cannot exceed Long Position Limit !")
                return
            if min_hedge > abs(short_limit):
                QMessageBox.warning(self, "Invalid Input", "Min Hedge cannot exceed abs(Short Position Limit) !")
                return
            # Validate Hedge Ratio
            if not (0 <= hedge_ratio <= 100):
                QMessageBox.warning(self, "Invalid Input", "Hedge Ratio must be between 0 and 100 !")
                return
            # Validate Take Profit
            if take_profit < 0:
                QMessageBox.warning(self, "Invalid Input", "Take Profit cannot be negative !")
                return
            # Validate Stop Loss
            if stop_loss > 0:
                QMessageBox.warning(self, "Invalid Input", "Stop Loss cannot be positive !")
                return
            # Validate Spread Check
            if not (0 <= spread_check <= 100):
                QMessageBox.warning(self, "Invalid Input", "Spread Check must be between 0 and 100 !")
                return

            changes = []
            def format_value(value):

                try:
                    # Önce int'e çevirme denemesi
                    ivalue = int(value)
                    # Eğer orijinali float gibi değilse (ör. "123.45" değilse)
                    if str(value).isdigit() or isinstance(value, int):
                        return f"{ivalue:,}"
                except ValueError:
                    pass  # int'e çevrilemiyorsa devam et
                return str(value)  # integer değilse olduğu gibi döndür
            
            if row in self.original_values:
                param_names = {
                    3: "Transfer Amount",
                    4: "Long Position Limit", 
                    5: "Short Position Limit",
                    6: "Min Hedge",
                    7: "Hedge Ratio",
                    8: "Take Profit",
                    9: "Stop Loss",
                    10: "Spread"
                }
                
                for col, param_name in param_names.items():
                    if col in self.original_values[row]:
                        old_value = self.original_values[row][col]
                        new_value = self.strategy_table.item(row, col).text()
                        if old_value != new_value:
                            changes.append(f"{param_name}: {old_value} → {format_value(new_value)}")
            
            # Update strategy data including status
            self.strategy_data[symbol] = {
                'status': current_status,
                'transfer_amc': transfer_amc,
                'long_limit': long_limit,
                'short_limit': short_limit,
                'min_hedge': min_hedge,
                'hedge_ratio': hedge_ratio/100,
                'take_profit': take_profit,
                'stop_loss': stop_loss,
                'spread_check': spread_check/100,


            }
                        
            # Send to Kafka via PanelStrategy topic
            self.send_strategy_update_to_kafka(symbol)
            
            # Exit edit mode
            self.exit_edit_mode(row)
            
            # Restore status cell background color after exit_edit_mode
            status_item = self.strategy_table.item(row, 2)
            if status_item:
                if current_status == "Running":
                    status_item.setBackground(QBrush(QColor(color_success)))
                else:
                    status_item.setBackground(QBrush(QColor("#FF7B63")))
            
            # Show changes in info window
            if changes:
                change_message = f"Strategy {symbol} updated:\n\n" + "\n".join(changes)
                msg=QMessageBox(self)
                msg.setWindowTitle("Parameters Changed")
                msg.setText(change_message)
            else:
                msg=QMessageBox(self)
                msg.setWindowTitle("Success")
                msg.setText(f"Strategy for {symbol} saved (no changes detected)")
            font=QFont("Roboto Mono",10,QFont.Weight.Bold)
            msg.setFont(font)
            msg.setStyleSheet(f"""
            QPushButton {{
                background-color: {color_theme}; color: white; border: 1px solid {color_theme};
                min-width: 10px; min-height: 10px; border-radius: 5px;
            }}
            QPushButton:hover {{ background-color: {color_theme_hover}; }}
            QPushButton:pressed {{ background-color: {color_theme_pressed}; }}
            """)
            # msg.setStyleSheet("""
            # QMessageBox {
            # background-color: ; #717B85  /* arka plan */
            # }
            # QLabel {
            #     background-color:#717B85 ;
            #     color: #white;               /* yazı */
            #     font-size: 10pt;
            # }
            # QPushButton {
            #     background-color: #00B19D;  /* buton rengi */
            #     color: white;
            #     padding: 5px 15px;
            #     border-radius: 4px;
            # }
            # QPushButton:hover {
            #     background-color: #009688;
            # }
            # """)
 
            msg.exec()    

        except Exception as e:
            print(f"Error saving strategy: {str(e)}")
            QMessageBox.warning(self, "Invalid Input", "All fields must be numeric values!")
    
    def cancel_edit(self):
        """Cancel editing and restore original values"""
        if self.editing_row is None:
            return
            
        row = self.editing_row
        
        # Store current status before restoring values
        status_item = self.strategy_table.item(row, 2)
        current_status = status_item.text() if status_item else "Stopped"
        
        # Restore original values
        if row in self.original_values:
            for col, value in self.original_values[row].items():
                item = self.strategy_table.item(row, col)
                if item:
                    item.setText(value)
        
        # Exit edit mode
        self.exit_edit_mode(row)
        
        # Restore status cell background color after exit_edit_mode
        status_item = self.strategy_table.item(row, 2)
        if status_item:
            if current_status == "Running":
                status_item.setBackground(QBrush(QColor(color_success )))
            else:
                status_item.setBackground(QBrush(QColor("#FF7B63")))
        
        print(f"Cancelled edit for row {row}")
    
    def exit_edit_mode(self, row):
        """Exit edit mode and restore normal appearance"""
        # Restore normal row appearance
        for col in range(self.strategy_table.columnCount()):
            item = self.strategy_table.item(row, col)
            if item:
                item.setBackground(QBrush(QColor("#0A1929")))  # Default background
                item.setForeground(QBrush(QColor("#ffffff")))  # Default text color
        
        # Disable editing for the table
        self.strategy_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        
        # Replace Save/Cancel buttons with Edit button
        symbol = self.strategy_table.item(row, 1).text() if self.strategy_table.item(row, 1) else ""
        edit_btn = QPushButton("Edit")
        edit_btn.setFixedSize(60, 20)
        edit_btn.setStyleSheet(f"""
            QPushButton {{
                background-color: {color_theme}; color: white; border: 1px solid {color_theme};
                padding: 2px 4px; border-radius: 2px; font-size: 10px; font-weight: bold;
            }}
            QPushButton:hover {{ background-color: {color_theme_hover}; }}
            QPushButton:pressed {{ background-color: {color_theme_pressed}; }}
        """)
        edit_btn.clicked.connect(lambda checked, symbol=symbol: self.edit_strategy_row_by_symbol(symbol))
        
        # Create container widget to center the button
        button_container = QWidget()
        button_layout = QHBoxLayout(button_container)
        button_layout.setContentsMargins(0, 0, 0, 0)
        button_layout.addStretch()
        button_layout.addWidget(edit_btn)
        button_layout.addStretch()
        
        self.strategy_table.setCellWidget(row, 11, button_container)
        
        # Clear editing state
        self.editing_row = None
        if row in self.original_values:
            del self.original_values[row]
        
        # Trigger a table update to refresh data from Kafka after exiting edit mode
        print("DEBUG: Exited edit mode - resuming Kafka updates")
        self.update_strategy_table()

    def delayed_table_population(self):
        """Populate tables after a delay to allow Kafka data to arrive"""
        print("Delayed table population - checking for Kafka data...")
        print(f"Current strategy data keys: {list(self.strategy_data.keys())}")
        
        # Populate tables with current data (either from Kafka or empty)
        self.update_position_table()
        self.update_risk_table()
        self.update_execution_table()
        self.update_customer_transaction_table() 
        self.update_provider_table()   

        # Populate overview tables
        self.update_position_pnl_table()
        self.update_customer_flow_table()
        self.update_execution_summary_table()
        self.update_currency_flow_table()

        
        print("Tables populated with current data")
    
    def send_strategy_update_to_kafka(self, symbol):
        """Send complete strategy update to Kafka PanelStrategy topic"""
        try:
            # Get current strategy data for this symbol
            if symbol in self.strategy_data:
                strategy = self.strategy_data[symbol]
                
                # Create CSV format string like the server expects
                # Format: symbol,status,transfer_amc,long_limit,short_limit,min_hedge,hedge_ratio,take_profit,stop_loss,spread_check
                # Convert status to lowercase string
                status_string = strategy.get('status', 'Stopped')

                strategy_str = f"{symbol},{status_string},{strategy['transfer_amc']},{strategy['long_limit']},{strategy['short_limit']},{strategy['min_hedge']},{strategy['hedge_ratio']},{strategy['take_profit']:.4f},{strategy['stop_loss']:.4f},{strategy['spread_check']},Python,{datetime.now().strftime('%H:%M:%S.%f')}"
                
                # Debug logging
                print(f"[STRATEGY UPDATE] Sending to Kafka: {strategy_str}")
                
                # Send to PanelStrategy topic
                success = self.kafka_worker.send_message('PanelStrategy_Sim2', symbol, strategy_str)
                
                if success:
                    print(f"✓ Sent complete strategy update to PanelStrategy: {symbol}")
                else:
                    print(f"✗ Failed to send strategy update for {symbol}")
            else:
                print(f"Strategy data not found for {symbol}")
                
        except Exception as e:
            print(f"Error sending strategy update: {e}")

    def send_manual_trade_input_to_kafka(self, manual_trade_input_data):
        """Send complete strategy update to Kafka PanelStrategy topic"""
        try:
           
            # Send to PanelManualTrade topic
            success = self.kafka_worker.send_message('PanelManualTrade_Sim2', '', manual_trade_input_data)
            
            if success:
                print(f"✓ Sent complete manual trade to PanelManualTrade: {manual_trade_input_data}")
            else:
                print(f"✗ Failed to send manual trade")
                
        except Exception as e:
            print(f"Error sending strategy update: {e}")

    def send_provider_update_to_kafka(self, symbol):
        """Send complete provider update to Kafka PanelProvider topic"""
        try:
            if symbol in self.provider_data:
                provider = self.provider_data[symbol]
                # burada provider dict’inde güncel değer neyse onu al
                kfh_gui_status   = provider.get("kfh_gui_status", "False")
                integral_gui_status = provider.get("integral_gui_status", "False")
                tradair_gui_status = provider.get("tradair_gui_status", "False")
                t360_gui_status   = provider.get("360t_gui_status", "False")
                # string formatı: Symbol,True,False,True,True
                provider_str = f"{symbol},{kfh_gui_status},{integral_gui_status},{tradair_gui_status},{t360_gui_status},Python,{datetime.now().strftime('%H:%M:%S.%f')}"
                print(f"[PROVIDER UPDATE] Sending to Kafka: {provider_str}")
                success = self.kafka_worker.send_message("PanelProvider_Sim2", symbol, provider_str)
                if success:
                    print(f"✓ Sent provider update for {symbol}")
                else:
                    print(f"✗ Failed to send provider update for {symbol}")
            else:
                print(f"Provider data not found for {symbol}")
        except Exception as e:
            print(f"Error sending provider update: {e}")

# Buttons

    def start_all_strategies(self):
        box = QMessageBox(self)
        box.setWindowTitle("Start All Strategies?")
        box.setText("Are you sure you want to start all strategies?")
        # Font belirle
        font=QFont("Roboto Mono",10,QFont.Weight.Bold)
        box.setFont(font)
        # Butonlar
        box.setStandardButtons(
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        box.setDefaultButton(QMessageBox.StandardButton.No)

        yes_btn = box.button(QMessageBox.StandardButton.Yes)
        no_btn  = box.button(QMessageBox.StandardButton.No)
        yes_btn.setStyleSheet(f"""
            QPushButton {{
                background-color: {color_theme}; color: white; border: 1px solid {color_theme};
                min-width: 10px; min-height: 10px; border-radius: 5px;
            }}
            QPushButton:hover {{ background-color: {color_theme_hover}; }}
            QPushButton:pressed {{ background-color: {color_theme_pressed}; }}
        """)
        no_btn.setStyleSheet(f"""
            QPushButton {{
                background-color: {color_fail}; color: white; border: 1px solid {color_fail};
                min-width: 10px; min-height: 10px; border-radius: 5px;
            }}
            QPushButton:hover {{ background-color: {color_fail_hover}; }}
            QPushButton:pressed {{ background-color: {color_fail_press}; }}
        """)


        reply = box.exec()
        if reply != QMessageBox.StandardButton.Yes:
            print("DEBUG: User cancelled start_all_strategies")
            return

        if self.waiting_for_strategy_kafka_confirmation:
            print(f"DEBUG: Ignoring start_all_strategies - waiting for Kafka confirmation")
            return
        
        # Set waiting flag to prevent further interactions
        self.waiting_for_strategy_kafka_confirmation = True
        
        # Track which strategies we're waiting for confirmation
        self.pending_strategy_confirmations = set()
        
        for i in range(self.strategy_table.rowCount()):
            # Update strategy data only (don't update GUI yet)
            symbol = self.strategy_table.item(i, 1).text()
            if symbol in self.strategy_data:
                self.strategy_data[symbol]['status'] = "Running"
                self.pending_strategy_confirmations.add(symbol)
        

        
        # Send each strategy individually to Kafka - GUI will be updated when confirmation is received
        for symbol in self.strategy_data:
            if self.strategy_data[symbol]['status'] == "Running":
                self.send_strategy_update_to_kafka(symbol)
        
        print(f"All strategies started - waiting for Kafka confirmation for {len(self.pending_strategy_confirmations)} strategies")

    def stop_all_strategies(self):
        box = QMessageBox(self)
        box.setWindowTitle("Stop All Strategies?")
        box.setText("Are you sure you want to stop all strategies?")
        # Font belirle
        font=QFont("Roboto Mono",10,QFont.Weight.Bold)
        box.setFont(font)
        # Butonlar
        box.setStandardButtons(
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        box.setDefaultButton(QMessageBox.StandardButton.No)

        yes_btn = box.button(QMessageBox.StandardButton.Yes)
        no_btn  = box.button(QMessageBox.StandardButton.No)
        yes_btn.setStyleSheet(f"""
            QPushButton {{
                background-color: {color_theme}; color: white; border: 1px solid {color_theme};
                min-width: 10px; min-height: 10px; border-radius: 5px;
            }}
            QPushButton:hover {{ background-color: {color_theme_hover}; }}
            QPushButton:pressed {{ background-color: {color_theme_pressed}; }}
        """)
        no_btn.setStyleSheet(f"""
            QPushButton {{
                background-color: {color_fail}; color: white; border: 1px solid {color_fail};
                min-width: 10px; min-height: 10px; border-radius: 5px;
            }}
            QPushButton:hover {{ background-color: {color_fail_hover}; }}
            QPushButton:pressed {{ background-color: {color_fail_press}; }}
        """)
        reply = box.exec()
        if reply != QMessageBox.StandardButton.Yes:
            print("DEBUG: User cancelled start_all_strategies")
            return


        """Stop all strategies - update data and send to Kafka, wait for confirmation"""
        # If we're already waiting for Kafka confirmation, ignore this update
        if self.waiting_for_strategy_kafka_confirmation:
            print(f"DEBUG: Ignoring stop_all_strategies - waiting for Kafka confirmation")
            return
        
        # Set waiting flag to prevent further interactions
        self.waiting_for_strategy_kafka_confirmation = True
        
        # Track which strategies we're waiting for confirmation
        self.pending_strategy_confirmations = set()
        
        for i in range(self.strategy_table.rowCount()):
            # Update strategy data only (don't update GUI yet)
            symbol = self.strategy_table.item(i, 1).text()
            if symbol in self.strategy_data:
                self.strategy_data[symbol]['status'] = "Stopped"
                self.pending_strategy_confirmations.add(symbol)
        
        # Send each strategy individually to Kafka - GUI will be updated when confirmation is received
        for symbol in self.strategy_data:
            if self.strategy_data[symbol]['status'] == "Stopped":
                self.send_strategy_update_to_kafka(symbol)
        
        print(f"All strategies stopped - waiting for Kafka confirmation for {len(self.pending_strategy_confirmations)} strategies")

    def on_add_manual_trade(self):
        """Read inputs and handle Manual Trade add"""
        symbol = self.manual_inputs["symbol"].currentText().strip().upper()
        side   = self.manual_inputs["side"].currentText().strip()
        price = self.manual_inputs["price"].text().strip()
        amount  = self.manual_inputs["amount"].text().strip()
        venue  = self.manual_inputs["venue"].currentText().strip()
        counterparty   = self.manual_inputs["counterparty"].text().strip()
        tradeid = self.manual_inputs["tradeid"].text().strip()
        user  = self.manual_inputs["user"].text().strip()

        # Basit doğrulama
        if not symbol or side not in ("Buy", "Sell") or not price:
            QMessageBox.warning(self, "Missing data", "Please fill Symbol, Side and Price.")
            return
        try:
            price = float(price)
        except ValueError:
            QMessageBox.warning(self, "Invalid price", "Price must be a number.")
            return
        try:
            amount = int(amount)
        except ValueError:
            QMessageBox.warning(self, "Invalid amount", "Price must be an integer.")
            return
        
        manual_trade_input_data = f"{symbol},{price},{side},{amount},{venue},{counterparty},{tradeid},{user},Python,{datetime.now().strftime('%H:%M:%S.%f')}"
        # (Opsiyonel) Kafka’ya/iş mantığınıza gönderin:
        # self.publish_manual_trade(payload)
        # Temizle & kullanıcıya bilgi
        self.manual_inputs["price"].clear()
        self.manual_inputs["amount"].clear()
        self.manual_inputs["venue"].clear()
        self.manual_inputs["counterparty"].clear()
        self.manual_inputs["tradeid"].clear()
        self.manual_inputs["side"].setCurrentIndex(-1)
        self.manual_inputs["symbol"].setCurrentIndex(-1)
        self.manual_inputs["venue"].setCurrentIndex(-1)

        self.send_manual_trade_input_to_kafka(manual_trade_input_data)
        QMessageBox.information(self, "Added", f"Manual trade added:\n{manual_trade_input_data}")

    def send_close_position_update_to_kafka(self, symbol):
        """Send complete strategy update to Kafka PanelClosePosition topic"""
        try:
            close_btn_str = f"{symbol},True,Python,{datetime.now().strftime('%H:%M:%S.%f')}"
            
            # Debug logging
            print(f"[CLOSE POSITION UPDATE] Sending to Kafka: {close_btn_str}")
            
            # Send to PanelStrategy topic
            success = self.kafka_worker.send_message('PanelClosePosition_Sim2', symbol, close_btn_str)
            
            if success:
                print(f"✓ Sent complete close position update to PanelClosePosition: {symbol}")
            else:
                print(f"✗ Failed to close position update for {symbol}")
                
        except Exception as e:
            print(f"Error sending close position update: {e}")

    def close_all_positions(self):
        """Close all positions and send Kafka messages"""
        row_count = self.position_table.rowCount()
        for row in range(row_count):
            symbol_item = self.position_table.item(row, 0)  # Sembol sütunu
            if symbol_item:
                symbol = symbol_item.text().strip()
                self.send_close_position_update_to_kafka(symbol)

        QMessageBox.information(self, "Close All", "Request has sent.")

    def clear_all_data_structures(self):
        """Clear all data structures to prevent duplicates"""
        self.position_data = {}
        self.risk_data = {}
        self.strategy_data = {}
        self.execution_data_list = []
        self.manual_trade_list = []
        self.position_pnl_data = {}
        self.customer_flow_data = {}
        self.execution_summary_data = {}
        self.currency_flow_data = {}
        self.provider_data = {}
        self.customer_transaction_data_list = []
        self.internal_transaction_data_list = []
        print("All data structures cleared")

    def ensure_unique_execution_data(self):
        """Ensure execution data list contains unique entries based on symbol, quantity, price, side, and timestamp"""
        if not hasattr(self, 'execution_data_list'):
            self.execution_data_list = []
            return
            
        # Create a set to track unique combinations
        seen_combinations = set()
        unique_executions = []
        
        for execution in self.execution_data_list:
            # Create a unique key based on execution details
            key = (
                execution.get('symbol', ''),
                execution.get('quantity', 0),
                execution.get('price', 0.0),
                execution.get('side', ''),
                execution.get('timestamp', '')
            )
            
            if key not in seen_combinations:
                seen_combinations.add(key)
                unique_executions.append(execution)
        
        self.execution_data_list = unique_executions
        #print(f"Execution data deduplicated: {len(unique_executions)} unique entries")

    def ensure_unique_manual_trade_data(self):
        """Ensure manual trade data list contains unique entries based on symbol, quantity, price, side, and timestamp"""
        if not hasattr(self, 'manual_trade_list'):
            self.manual_trade_list = []
            return
            
        # Create a set to track unique combinations
        seen_combinations = set()
        unique_manual_trades = []
        
        for manual_trade in self.manual_trade_list:
            # Create a unique key based on execution details
            key = (
                manual_trade.get('symbol', ''),
                manual_trade.get('quantity', 0),
                manual_trade.get('price', 0.0),
                manual_trade.get('side', ''),
                manual_trade.get('timestamp', '')
            )
            
            if key not in seen_combinations:
                seen_combinations.add(key)
                unique_manual_trades.append(manual_trade)
        
        self.manual_trade_list = unique_manual_trades

    def ensure_unique_customer_transaction_data(self):
        """Ensure execution data list contains unique entries based on symbol, quantity, price, side, and timestamp"""
        if not hasattr(self, 'customer_transaction_data_list'):
            self.customer_transaction_data_list = []
            return
            
        # Create a set to track unique combinations
        seen_combinations = set()
        unique_customer_transactions = []
        
        for customer_transaction in self.customer_transaction_data_list:
            # Create a unique key based on execution details
            key = (
                customer_transaction.get('symbol', ''),
                customer_transaction.get('quantity', 0.0),
                customer_transaction.get('customerid', ''),
                customer_transaction.get('timestamp', ''),
                customer_transaction.get('tranid', ''),
            )
            
            if key not in seen_combinations:
                seen_combinations.add(key)
                unique_customer_transactions.append(customer_transaction)
        
        self.customer_transaction_data_list = unique_customer_transactions

    def ensure_unique_internal_transaction_data(self):
        """Ensure execution data list contains unique entries based on symbol, quantity, price, side, and timestamp"""
        if not hasattr(self, 'internal_transaction_data_list'):
            self.internal_transaction_data_list = []
            return
            
        # Create a set to track unique combinations
        seen_combinations = set()
        unique_internal_transactions = []
        
        for internal_transaction in self.internal_transaction_data_list:
            # Create a unique key based on execution details
            key = (
                internal_transaction.get('symbol', ''),
                internal_transaction.get('quantity', 0.0),
                internal_transaction.get('customerid', ''),
                internal_transaction.get('timestamp', ''),
                internal_transaction.get('tranid', ''),
            )
            
            if key not in seen_combinations:
                seen_combinations.add(key)
                unique_internal_transactions.append(internal_transaction)
        
        self.internal_transaction_data_list = unique_internal_transactions

    def create_close_position_button_widget(self, row, symbol):
        """Create a close position button widget for position table row"""
        close_btn = QPushButton("Close")
        close_btn.setFixedSize(60, 20)
        close_btn.setStyleSheet(f"""
            QPushButton {{
                background-color: {color_theme}; color: white; border: 1px solid {color_theme};
                padding: 2px 4px; border-radius: 2px; font-size: 10px; font-weight: bold;
            }}
            QPushButton:hover {{ background-color: {color_theme_hover}; }}
            QPushButton:pressed {{ background-color: {color_theme_pressed}; }}
        """)
        # Connect with dynamic row index and symbol
        close_btn.clicked.connect(lambda checked, row=row, symbol=symbol: self.send_close_position_update_to_kafka(symbol))
          
        # Create container widget to center the button
        button_container = QWidget()
        button_layout = QHBoxLayout(button_container)
        button_layout.setContentsMargins(0, 0, 0, 0)
        button_layout.addStretch()
        button_layout.addWidget(close_btn)
        button_layout.addStretch()
        
        return button_container

    def update_strategy_table_gui_from_kafka(self, symbol, status):
        """Update the strategy table GUI with the confirmed status from Kafka"""
        row = -1
        for i in range(self.strategy_table.rowCount()):
            if self.strategy_table.item(i, 1) and self.strategy_table.item(i, 1).text() == symbol:
                row = i
                break
        
        if row != -1:
            # Update status column
            status_item = QTableWidgetItem(status.capitalize())
            status_item.setFlags(status_item.flags() & ~Qt.ItemFlag.ItemIsEditable)  # Make non-editable
            if status.lower() == "running":
                status_item.setBackground(QBrush(QColor(color_success )))
            else:
                status_item.setBackground(QBrush(QColor("#FF7B63")))
            self.strategy_table.setItem(row, 2, status_item)
        switch_container = self.strategy_table.cellWidget(row, 0)
        if switch_container:
        # Container içindeki Switch'i bul
            for child in switch_container.findChildren(Switch):
                if status.lower() == "running":
                    child._checked = True
                    child._handle_position = child.width() - 20
                else:
                    child._checked = False
                    child._handle_position = 2
                child.animate()
                child.update()
                break  # ilk switch bulunduğunda çık
        else:
            print(f"DEBUG: Could not find row for symbol {symbol} in strategy table")
        
        # Remove this symbol from pending confirmations
        if hasattr(self, 'pending_strategy_confirmations') and symbol in self.pending_strategy_confirmations:
            self.pending_strategy_confirmations.remove(symbol)
            print(f"DEBUG: Removed {symbol} from pending confirmations. Remaining: {len(self.pending_strategy_confirmations)}")
            
            # Only reset waiting flag when all pending confirmations are received
            if len(self.pending_strategy_confirmations) == 0:
                self.waiting_for_strategy_kafka_confirmation = False
                print(f"DEBUG: All strategy confirmations received. Reset waiting_for_strategy_kafka_confirmation flag")
            else:
                print(f"DEBUG: Still waiting for {len(self.pending_strategy_confirmations)} more confirmations")
        else:
            # Fallback for individual updates that don't use tracking
            self.waiting_for_strategy_kafka_confirmation = False
            print(f"DEBUG: Reset waiting_for_strategy_kafka_confirmation flag after confirmation for {symbol}")

           # ✅ Pending listeden çıkar
        if hasattr(self, 'pending_strategy_confirmations') and symbol in self.pending_strategy_confirmations:
            self.pending_strategy_confirmations.remove(symbol)
            print(f"DEBUG: Removed {symbol} from pending strategy confirmations")
        # ✅ Eğer pending boşaldıysa flag’i sıfırla
        if hasattr(self, 'pending_strategy_confirmations') and len(self.pending_strategy_confirmations) == 0:
            self.waiting_for_strategy_kafka_confirmation = False
            print("DEBUG: All strategy confirmations received. Reset waiting_for_strategy_kafka_confirmation flag")
        # Pending listeden çıkar ve flag resetle
        if hasattr(self, 'pending_strategy_confirmations') and symbol in self.pending_strategy_confirmations:
            self.pending_strategy_confirmations.remove(symbol)
        if not self.pending_strategy_confirmations:
            self.waiting_for_strategy_kafka_confirmation = False
            print("DEBUG: Strategy confirmation complete, reset waiting flag.")

    def update_provider_table_gui_from_kafka(self, symbol, provider_name, status):
        # Symbol sütununu bul
        row = -1
        for i in range(self.provider_table.rowCount()):
            if self.provider_table.item(i, 0) and self.provider_table.item(i, 0).text() == symbol:
                row = i
                break
        if row == -1:
            print(f"DEBUG: Symbol {symbol} not found in table")
            return
        # Provider eşleme
        provider_map = {
            "KFH": ("kfh_gui_status", 1),
            "Integral": ("integral_gui_status", 2),
            "Tradair": ("tradair_gui_status", 3),
            "360T": ("360t_gui_status", 4),
        }
        if provider_name not in provider_map:
            print(f"DEBUG: Unknown provider {provider_name}")
            return
        col_name, col = provider_map[provider_name]
        switch_container = self.provider_table.cellWidget(row, col)
        if switch_container:
            switches = switch_container.findChildren(Switch)
            switch_widget = switches[0] if switches else None
            if switch_widget:
                if status.lower() == "true":
                    switch_widget._checked = True
                    switch_widget._handle_position = switch_widget.width() - 20
                else:
                    switch_widget._checked = False
                    switch_widget._handle_position = 2
                switch_widget.update()
                switch_widget.animate()
        else:
            print(f"DEBUG: No widget found in cell ({row}, {col})")
        # Pending provider confirmations temizle
        if hasattr(self, 'pending_provider_confirmations') and (symbol, provider_name) in self.pending_provider_confirmations:
            self.pending_provider_confirmations.remove((symbol, provider_name))
            print(f"DEBUG: Removed {(symbol, provider_name)} from pending confirmations")
        # Eğer pending boşaldıysa flag reset
        if not getattr(self, 'pending_provider_confirmations', []):
            self.waiting_for_provider_kafka_confirmation = False
            print("DEBUG: Provider confirmation complete, reset waiting flag.")

    def create_strategy_switch_widget(self, row, symbol, initial_status="Stopped"):
        """Create a switch widget for strategy start/stop control"""
        switch = Switch()
        switch.setFixedSize(48, 20)
        # İlk durumu zorunlu parametre olarak al ve uygula
        switch._checked = True if initial_status == "Running" else False
        switch._handle_position = switch.width() - 20 if switch._checked else 2
        def on_switch_toggled():
            new_status = "Running" if switch._checked else "Stopped"
            print(f"Strategy {new_status} for row: {row}, symbol: {symbol}")
            self.update_strategy_status(row, switch)
        # Mevcut mousePressEvent'e ekleme yap
        original_mouse_event = switch.mousePressEvent
        def new_mouse_event(event):
            original_mouse_event(event)  # Animasyon ve toggle çalışmaya devam etsin
            on_switch_toggled()
        switch.mousePressEvent = new_mouse_event
        # Ortalamak için container
        container = QWidget()
        layout = QHBoxLayout(container)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addStretch()
        layout.addWidget(switch)
        layout.addStretch()
        return container
 
    def create_provider_switch_widget(self, row, column, symbol, initial_status="True"):
        """Create a switch widget for provider active/inactive control"""
        switch = Switch()
        switch.setFixedSize(48, 20)
        # İlk durumu zorunlu parametre olarak al ve uygula
        switch._checked = True if initial_status == "True" else False
        switch._handle_position = switch.width() - 20 if switch._checked else 2
        def on_switch_toggled():
            new_status = "True" if switch._checked else "False"
            print(f"Provider {new_status} for row: {row}, for column: {column},symbol: {symbol}")
            self.update_provider_status(row, column, switch)
        # Mevcut mousePressEvent'e ekleme yap
        original_mouse_event = switch.mousePressEvent
        def new_mouse_event(event):
            original_mouse_event(event)  # Animasyon ve toggle çalışmaya devam etsin
            on_switch_toggled()
        switch.mousePressEvent = new_mouse_event
        # Ortalamak için container
        container = QWidget()
        layout = QHBoxLayout(container)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addStretch()
        layout.addWidget(switch)
        layout.addStretch()
        return container

    def _get_selected_key(self, table):
            sel = table.selectionModel()
            if not sel:
                return None
            rows = sel.selectedRows(0)  # Symbol kolonu
            if not rows:
                return None
            it = table.item(rows[0].row(), 0)
            return it.data(Qt.ItemDataRole.UserRole) if it else None
    
    def _reselect_by_key(self, table, key):
        if not key:
            return
        if QApplication.focusWidget() is not table:
            return

        for r in range(table.rowCount()):
            it = table.item(r, 0)
            if it and it.data(Qt.ItemDataRole.UserRole) == key:
                table.selectRow(r)
                table.setCurrentCell(r, 0)
                break

    def _attach_focus_behavior(self, table):
        orig_focus_in = table.focusInEvent
        orig_focus_out = table.focusOutEvent
 
        def focus_in(event):
        # sadece bu tabloda seçim kalacak
            orig_focus_in(event)
 
        def focus_out(event):
            # odak kaybolunca highlight temizle
            table.clearSelection()
            orig_focus_out(event)
    
        table.focusInEvent = focus_in
        table.focusOutEvent = focus_out

    
def main():
    app = QApplication(sys.argv)
 
    window = TradingApp()
    window.show()
    sys.exit(app.exec())
if __name__ == "__main__":
    main()
