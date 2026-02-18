import subprocess
import os
import logging

logger = logging.getLogger(__name__)

def get_uci_options(engine_path):
    """
    Ejecuta el motor, envía 'uci' y parsea las opciones disponibles.
    Devuelve un diccionario con la estructura:
    {
        "Engine Name": str,
        "Author": str,
        "Options": {
            "OptionName": {
                "type": "spin|check|combo|string|button",
                "default": val,
                "min": val, # solo spin
                "max": val, # solo spin
                "var": []   # solo combo
            },
            ...
        }
    }
    """
    if not engine_path or not os.path.exists(engine_path):
        return None

    try:
        # Iniciamos el proceso
        process = subprocess.Popen(
            [engine_path],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1
        )

        # Enviamos el comando 'uci'
        process.stdin.write("uci\n")
        process.stdin.flush()

        info = {
            "id": {"name": "Unknown", "author": "Unknown"},
            "options": {}
        }

        while True:
            line = process.stdout.readline()
            if not line:
                break
            
            line = line.strip()
            if line == "uciok":
                break
            
            parts = line.split()
            if not parts:
                continue

            if parts[0] == "id":
                # id name Stockfish 15
                # id author ...
                if len(parts) > 2:
                    key = parts[1]
                    value = " ".join(parts[2:])
                    info["id"][key] = value
            
            elif parts[0] == "option":
                # option name Hash type spin default 16 min 1 max 33554432
                # option name Ponder type check default false
                parse_uci_option(parts, info["options"])

        process.stdin.write("quit\n")
        process.stdin.flush()
        process.wait(timeout=2)
        return info

    except Exception as e:
        logger.error(f"Error leyendo opciones UCI de {engine_path}: {e}")
        return None

def parse_uci_option(parts, options_dict):
    """Ayudante para parsear una línea de opción UCI."""
    # Ejemplo: ['option', 'name', 'Hash', 'type', 'spin', 'default', '16', 'min', '1', 'max', '128000']
    
    # Encontrar índices clave
    try:
        name_idx = parts.index("name") + 1
        type_idx = parts.index("type")
    except ValueError:
        return

    # El nombre puede tener espacios: "option name Multi PV type spin..."
    name = " ".join(parts[name_idx:type_idx])
    opt_type = parts[type_idx + 1]
    
    opt_data = {"type": opt_type}
    
    # Extraer default
    if "default" in parts:
        def_idx = parts.index("default")
        # El valor por defecto puede ser el resto de la línea o hasta otra keyword
        # Pero en UCI standard: default <x> [min <y> max <z>]
        # Para string/combo puede ser complejo.
        
        # Simplificación: iteramos buscando keywords conocidas
        rest = parts[def_idx+1:]
        val_parts = []
        for p in rest:
            if p in ["min", "max", "var"]:
                break
            val_parts.append(p)
        
        default_val = " ".join(val_parts)
        
        # Conversión de tipos
        if opt_type == "spin":
            try: opt_data["default"] = int(default_val)
            except: opt_data["default"] = 0
        elif opt_type == "check":
            opt_data["default"] = (default_val.lower() == "true")
        else:
            opt_data["default"] = default_val

    # Extraer min/max para spin
    if opt_type == "spin":
        if "min" in parts:
            try: opt_data["min"] = int(parts[parts.index("min") + 1])
            except: pass
        if "max" in parts:
            try: opt_data["max"] = int(parts[parts.index("max") + 1])
            except: pass

    # Extraer var para combo
    if opt_type == "combo":
        opt_data["vars"] = []
        # var puede aparecer múltiples veces
        # option name Style type combo default Normal var Solid var Normal var Risky
        indices = [i for i, x in enumerate(parts) if x == "var"]
        for i in indices:
            # El valor es lo que hay entre este 'var' y el siguiente keyword o fin de linea
            # En spec UCI estándar: var <string>
            # Asumimos una palabra o manejamos espacios con cuidado si fuera necesario, 
            # pero típicamente es una palabra.
            if i + 1 < len(parts):
                opt_data["vars"].append(parts[i+1])

    options_dict[name] = opt_data
