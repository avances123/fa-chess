import pytest
import importlib

def test_package_imports():
    """Verifica que todos los módulos son importables usando el prefijo src."""
    modules = [
        "src.main",
        "src.cli",
        "src.config",
        "src.converter",
        "src.core.db_manager",
        "src.core.eco",
        "src.core.workers",
        "src.ui.main_window",
        "src.ui.board",
        "src.ui.widgets.eval_graph",
        "src.ui.widgets.results_bar",
        "src.ui.widgets.analysis_report"
    ]
    
    for mod_name in modules:
        try:
            importlib.import_module(mod_name)
        except ImportError as e:
            pytest.fail(f"Fallo crítico al importar {mod_name}: {e}. Revisar prefijos 'src.'")
