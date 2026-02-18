from PySide6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QListWidget, QPushButton, 
    QLabel, QLineEdit, QWidget, QScrollArea, QCheckBox, QSpinBox, 
    QComboBox, QFileDialog, QMessageBox, QFormLayout, QGroupBox, QTabWidget
)
from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QColor, QFont
import os
import copy
from src.core.engine_utils import get_uci_options
from src.config import logger

class EngineOptionsWidget(QWidget):
    """
    Genera dinámicamente controles para las opciones UCI.
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        self.layout = QFormLayout(self)
        self.current_options = {} # {name: value}
        self.schema = {}          # {name: schema_dict}
        self.widgets = {}         # {name: widget}

    def build(self, schema, current_values):
        """
        Construye la UI basada en el esquema UCI y valores actuales.
        schema: dict devuelto por get_uci_options()["options"]
        current_values: dict {option_name: value} guardado en config
        """
        # Limpiar layout anterior
        while self.layout.count():
            item = self.layout.takeAt(0)
            widget = item.widget()
            if widget: widget.deleteLater()
        
        self.schema = schema
        self.current_values = current_values
        self.widgets = {}

        # Ordenar alfabéticamente para consistencia, o priorizar algunos comunes
        common_order = ["Threads", "Hash", "MultiPV", "Skill Level"]
        sorted_keys = sorted(schema.keys(), key=lambda x: (0 if x in common_order else 1, x))

        for name in sorted_keys:
            opt_def = schema[name]
            opt_type = opt_def.get("type")
            
            # Valor actual > valor por defecto del esquema > valor por defecto hardcoded
            val = current_values.get(name, opt_def.get("default"))
            
            widget = None
            
            if opt_type == "check":
                widget = QCheckBox()
                widget.setChecked(bool(val))
            
            elif opt_type == "spin":
                widget = QSpinBox()
                # UCI spin range
                min_val = opt_def.get("min", 0)
                max_val = opt_def.get("max", 99999)
                # QSpinBox limite de int32, UCI puede ser mayor, pero PySide suele manejarlo bien
                # aunque max 2^31-1. Hash puede ser grande.
                widget.setRange(min_val, min(max_val, 2147483647)) 
                try:
                    widget.setValue(int(val))
                except: widget.setValue(min_val)
            
            elif opt_type == "combo":
                widget = QComboBox()
                vars = opt_def.get("vars", [])
                widget.addItems(vars)
                if val in vars:
                    widget.setCurrentText(val)
            
            elif opt_type == "string":
                widget = QLineEdit()
                widget.setText(str(val))
            
            elif opt_type == "button":
                # Los botones UCI ejecutan una acción, no guardan estado. 
                # Scid suele ignorarlos en configuración persistente o poner un botón.
                # Lo omitiremos por ahora para persistencia.
                continue

            if widget:
                # Tooltip con info extra
                tooltip = f"Type: {opt_type}"
                if "min" in opt_def: tooltip += f", Min: {opt_def['min']}"
                if "max" in opt_def: tooltip += f", Max: {opt_def['max']}"
                widget.setToolTip(tooltip)
                
                self.widgets[name] = widget
                self.layout.addRow(name, widget)

    def get_options(self):
        """Recupera los valores actuales de los widgets."""
        options = {}
        for name, widget in self.widgets.items():
            if isinstance(widget, QCheckBox):
                options[name] = widget.isChecked()
            elif isinstance(widget, QSpinBox):
                options[name] = widget.value()
            elif isinstance(widget, QComboBox):
                options[name] = widget.currentText()
            elif isinstance(widget, QLineEdit):
                options[name] = widget.text()
        return options


class EngineManagerDialog(QDialog):
    engines_changed = Signal() # Emitida al guardar cambios

    def __init__(self, config_service, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Gestor de Motores y Análisis")
        self.resize(800, 600)
        self.config = config_service
        
        # Datos internos
        self.engines_list = copy.deepcopy(self.config.get_engines())
        self.active_engine_name = self.config.get("active_engine")
        
        self.current_engine_idx = -1
        
        self.init_ui()
        self.populate_list()

    def init_ui(self):
        # Layout principal del diálogo
        self.main_layout = QVBoxLayout(self)
        
        # Crear Tabs
        self.tabs = QTabWidget()
        
        # --- TAB 1: GESTIÓN DE MOTORES ---
        self.tab_engines = QWidget()
        self.init_engines_tab(self.tab_engines)
        self.tabs.addTab(self.tab_engines, "Motores Instalados")
        
        # --- TAB 2: LÍMITES GLOBALES ---
        self.tab_limits = QWidget()
        self.init_limits_tab(self.tab_limits)
        self.tabs.addTab(self.tab_limits, "Configuración de Análisis")
        
        self.main_layout.addWidget(self.tabs)

        # --- Botones Dialogo (Globales) ---
        dlg_btns = QHBoxLayout()
        dlg_btns.addStretch()
        save_btn = QPushButton("Guardar Todo")
        save_btn.clicked.connect(self.save_all)
        cancel_btn = QPushButton("Cancelar")
        cancel_btn.clicked.connect(self.reject)
        dlg_btns.addWidget(save_btn)
        dlg_btns.addWidget(cancel_btn)
        
        self.main_layout.addLayout(dlg_btns)

    def init_limits_tab(self, parent_widget):
        layout = QFormLayout(parent_widget)
        
        # Cargar valores actuales
        current_depth = self.config.get("engine_depth")
        current_tree_depth = self.config.get("tree_depth")
        
        self.spin_depth = QSpinBox()
        self.spin_depth.setRange(0, 99)
        self.spin_depth.setValue(current_depth if current_depth is not None else 20)
        self.spin_depth.setSpecialValueText("Infinito")
        self.spin_depth.setToolTip("Profundidad máxima para el análisis principal (Infinito o Ctrl+Espacio). 0 = Sin límite.")
        
        self.spin_tree_depth = QSpinBox()
        self.spin_tree_depth.setRange(1, 40)
        self.spin_tree_depth.setValue(current_tree_depth if current_tree_depth is not None else 12)
        self.spin_tree_depth.setToolTip("Profundidad rápida para rellenar las estadísticas del árbol de aperturas.")
        
        layout.addRow("Profundidad Análisis Infinito:", self.spin_depth)
        layout.addRow("Profundidad Análisis Árbol:", self.spin_tree_depth)
        
        info_lbl = QLabel("\nNota: Estos límites se aplican a CUALQUIER motor que selecciones como activo.")
        info_lbl.setStyleSheet("color: #666; font-style: italic;")
        layout.addRow(info_lbl)

    def init_engines_tab(self, parent_widget):
        main_hbox = QHBoxLayout(parent_widget)
        
        # --- Panel Izquierdo ---
        left_panel = QWidget()
        left_vbox = QVBoxLayout(left_panel)
        self.list_widget = QListWidget()
        self.list_widget.currentRowChanged.connect(self.on_engine_selected)
        
        btns_hbox = QHBoxLayout()
        add_btn = QPushButton("Añadir...")
        add_btn.clicked.connect(self.add_engine)
        del_btn = QPushButton("Eliminar")
        del_btn.clicked.connect(self.delete_engine)
        btns_hbox.addWidget(add_btn)
        btns_hbox.addWidget(del_btn)
        
        # Botón para activar motor
        self.activate_btn = QPushButton("✅ Usar este Motor")
        self.activate_btn.clicked.connect(self.set_active_engine)
        self.activate_btn.setEnabled(False)
        self.activate_btn.setStyleSheet("font-weight: bold; padding: 5px;")
        
        left_vbox.addWidget(QLabel("Motores:"))
        left_vbox.addWidget(self.list_widget)
        left_vbox.addWidget(self.activate_btn)
        left_vbox.addLayout(btns_hbox)
        
        main_hbox.addWidget(left_panel, 1)
        
        # --- Panel Derecho ---
        right_panel = QScrollArea()
        right_panel.setWidgetResizable(True)
        right_content = QWidget()
        self.right_vbox = QVBoxLayout(right_content)
        
        # Info Gral
        info_group = QGroupBox("Propiedades del Motor")
        info_form = QFormLayout()
        self.name_edit = QLineEdit()
        self.name_edit.editingFinished.connect(self.save_current_state_temp)
        
        path_layout = QHBoxLayout()
        self.path_edit = QLineEdit()
        self.path_edit.setReadOnly(True)
        browse_btn = QPushButton("...")
        browse_btn.setFixedWidth(30)
        browse_btn.clicked.connect(self.browse_path)
        path_layout.addWidget(self.path_edit)
        path_layout.addWidget(browse_btn)
        
        info_form.addRow("Nombre:", self.name_edit)
        info_form.addRow("Ruta:", path_layout)
        info_group.setLayout(info_form)
        
        # Opciones
        self.options_group = QGroupBox("Opciones UCI")
        opt_layout = QVBoxLayout()
        self.options_widget = EngineOptionsWidget()
        
        reload_btn = QPushButton("Recargar Opciones (Ejecutar motor)")
        reload_btn.clicked.connect(self.reload_options)
        
        opt_layout.addWidget(reload_btn)
        opt_layout.addWidget(self.options_widget)
        self.options_group.setLayout(opt_layout)
        
        self.right_vbox.addWidget(info_group)
        self.right_vbox.addWidget(self.options_group)
        self.right_vbox.addStretch()
        
        right_panel.setWidget(right_content)
        main_hbox.addWidget(right_panel, 2)

    def populate_list(self):
        self.list_widget.clear()
        for eng in self.engines_list:
            name = eng["name"]
            display_text = name
            if name == self.active_engine_name:
                display_text = f"👉 {name} (Activo)"
                
            item = os.path.basename(eng["path"]) # solo info extra interna
            self.list_widget.addItem(display_text)
            
            # Decoración extra
            if name == self.active_engine_name:
                item = self.list_widget.item(self.list_widget.count() - 1)
                font = item.font()
                font.setBold(True)
                item.setFont(font)
                item.setForeground(QColor("#2e7d32")) # Verde

    def on_engine_selected(self, row):
        if self.current_engine_idx != -1 and self.current_engine_idx < len(self.engines_list):
            self.save_current_to_memory()

        if row < 0 or row >= len(self.engines_list):
            self.right_vbox.setEnabled(False)
            self.activate_btn.setEnabled(False)
            self.current_engine_idx = -1
            return
        
        self.right_vbox.setEnabled(True)
        self.activate_btn.setEnabled(True)
        self.current_engine_idx = row
        eng = self.engines_list[row]
        
        self.name_edit.setText(eng["name"])
        self.path_edit.setText(eng["path"])
        
        schema = eng.get("uci_schema", {})
        current_vals = eng.get("options", {})
        self.options_widget.build(schema, current_vals)

    def set_active_engine(self):
        if self.current_engine_idx != -1:
            name = self.engines_list[self.current_engine_idx]["name"]
            self.active_engine_name = name
            
            # Guardamos estado actual para refrescar la lista
            self.save_current_to_memory()
            
            # Refrescar lista para ver el cambio visual
            current_row = self.current_engine_idx
            self.populate_list()
            self.list_widget.setCurrentRow(current_row)

    def save_current_to_memory(self):
        if self.current_engine_idx == -1: return
        idx = self.current_engine_idx
        # IMPORTANTE: Si cambiamos el nombre en el edit, actualizarlo en la lista y en active_engine_name si coincide
        old_name = self.engines_list[idx]["name"]
        new_name = self.name_edit.text()
        
        self.engines_list[idx]["name"] = new_name
        self.engines_list[idx]["options"] = self.options_widget.get_options()
        
        if self.active_engine_name == old_name:
            self.active_engine_name = new_name
            
        # Actualizar texto del item en la lista (si no regeneramos toda la lista)
        # Pero populate_list es mejor para el estado activo.

    def save_current_state_temp(self):
        self.save_current_to_memory()
        # Refrescar lista por si cambió el nombre
        row = self.list_widget.currentRow()
        self.populate_list()
        self.list_widget.setCurrentRow(row)

    def add_engine(self):
        path, _ = QFileDialog.getOpenFileName(self, "Seleccionar Motor de Ajedrez")
        if not path: return
        
        info = get_uci_options(path)
        if not info:
            QMessageBox.warning(self, "Error", "No es un motor UCI válido o falló la ejecución.")
            return
            
        name = info["id"].get("name", os.path.basename(path))
        new_engine = {
            "name": name,
            "path": path,
            "options": {},
            "uci_schema": info["options"]
        }
        self.engines_list.append(new_engine)
        self.populate_list()
        self.list_widget.setCurrentRow(len(self.engines_list) - 1)
        
        # Si es el primero, hacerlo activo por defecto
        if len(self.engines_list) == 1:
            self.active_engine_name = name
            self.populate_list()
            self.list_widget.setCurrentRow(0)

    def browse_path(self):
        if self.current_engine_idx == -1: return
        path, _ = QFileDialog.getOpenFileName(self, "Seleccionar Motor de Ajedrez")
        if path:
            self.path_edit.setText(path)
            self.engines_list[self.current_engine_idx]["path"] = path
            self.reload_options()

    def delete_engine(self):
        row = self.list_widget.currentRow()
        if row >= 0:
            deleted_name = self.engines_list[row]["name"]
            del self.engines_list[row]
            
            if self.active_engine_name == deleted_name:
                self.active_engine_name = None
                if self.engines_list:
                    self.active_engine_name = self.engines_list[0]["name"]
            
            self.populate_list()
            self.current_engine_idx = -1
            self.name_edit.clear()
            self.path_edit.clear()
            self.options_widget.build({}, {})

    def reload_options(self):
        if self.current_engine_idx == -1: return
        path = self.path_edit.text()
        info = get_uci_options(path)
        if info:
            self.engines_list[self.current_engine_idx]["uci_schema"] = info["options"]
            current_vals = self.options_widget.get_options()
            self.options_widget.build(info["options"], current_vals)
            QMessageBox.information(self, "Éxito", "Opciones recargadas del motor.")
        else:
            QMessageBox.critical(self, "Error", "No se pudo cargar la info UCI.")

    def save_all(self):
        # 1. Guardar cambios en la lista de motores
        self.save_current_to_memory()
        self.config.set("engines", self.engines_list)
        
        # Guardar motor activo explícito
        if self.active_engine_name:
            self.config.set_active_engine(self.active_engine_name)
        elif self.engines_list:
            # Fallback
            self.config.set_active_engine(self.engines_list[0]["name"])
        else:
            self.config.set_active_engine(None)
            
        # 2. Guardar límites globales
        self.config.set("engine_depth", self.spin_depth.value())
        self.config.set("tree_depth", self.spin_tree_depth.value())
        
        self.accept()
