import chess
import os
import re

class ECOManager:
    def __init__(self, eco_path):
        self.openings = [] # Lista de (linea_uci, nombre)
        if os.path.exists(eco_path):
            self.load_eco(eco_path)

    def load_eco(self, path):
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    line = line.strip()
                    # Formato Scid: A00 "Nombre" 1. e4 ...
                    if not line or line.startswith("["): continue
                    
                    if '"' in line:
                        parts = line.split('"')
                        if len(parts) < 3: continue
                        
                        name = parts[1]
                        moves_part = parts[2].strip()
                        
                        # 1. Limpiar comentarios { ... } y ( ... )
                        moves_part = re.sub(r'\{.*?\}', '', moves_part)
                        moves_part = re.sub(r'\(.*?\)', '', moves_part)
                        
                        # 2. Limpiar números de jugada (1.e4, 1. e4, 2...)
                        clean_moves = re.sub(r'\d+\.+\s*', '', moves_part)
                        # Limpiar símbolos como !, ?, +
                        clean_moves = re.sub(r'[\!\?\+\#]', '', clean_moves)
                        
                        try:
                            board = chess.Board()
                            uci_moves = []
                            for san in clean_moves.split():
                                if not san or san == "*": continue
                                try:
                                    move = board.push_san(san)
                                    uci_moves.append(move.uci())
                                except:
                                    break # Si una jugada falla, paramos esa línea
                            
                            line_uci_str = " ".join(uci_moves)
                            if line_uci_str: # Solo añadir si tiene jugadas
                                self.openings.append((line_uci_str, name))
                        except:
                            continue
                            
            # Ordenar por longitud de jugadas (más larga/específica primero)
            self.openings.sort(key=lambda x: len(x[0].split()), reverse=True)
        except Exception as e:
            print(f"Error cargando ECO: {e}")

    def get_opening_name(self, current_line_uci):
        if not current_line_uci:
            return "Posición Inicial"
            
        # Buscamos la coincidencia más específica
        for line_uci, name in self.openings:
            # Comprobamos si la línea actual empieza con la línea del ECO
            # o si la línea del ECO empieza con la actual (coincidencia parcial)
            if current_line_uci.startswith(line_uci):
                return name
                
        return "Variante Desconocida"