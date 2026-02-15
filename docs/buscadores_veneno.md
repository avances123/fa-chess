# 🧪 Buscador de Veneno: Teoría vs. Práctica

El **Buscador de Veneno** es una herramienta exclusiva de `fa-chess` diseñada para identificar jugadas que son estadísticamente más exitosas de lo que su evaluación teórica sugiere.

## ¿Qué significa el icono 🧪?

Cuando ves una poción al lado de un movimiento en el árbol de aperturas, significa que esa jugada es **"Venenosa"**. 

A diferencia del motor de análisis (que busca la verdad absoluta), el Buscador de Veneno busca la **verdad práctica**: posiciones donde los humanos suelen equivocarse.

### Los dos tipos de Veneno:

1. **La Trampa (Celada Dinámica):**
   - **Criterio:** La evaluación del motor es mala (ej. -0.70) pero el bando que mueve gana más de la mitad de las partidas (>52%).
   - **Significado:** Es un "anzuelo". El motor sabe castigarlo, pero la mayoría de los humanos no encuentran la respuesta correcta y acaban perdiendo.

2. **El Oro Práctico (Dificultad de Juego):**
   - **Criterio:** La evaluación es de tablas (0.00) pero un bando gana masivamente (>60%).
   - **Significado:** La posición es teóricamente igualada, pero es mucho más fácil de jugar para un bando que para el otro.

## Ejemplos Reales

### 1. Gambito Stafford (`1.e4 e5 2.Nf3 Nf6 3.Nxe5 Nc6`)
- **Eval:** -1.50 (Muy malo para el negro).
- **Win Rate:** ~60% (El negro gana muchísimo).
- **Conclusión:** Es puro veneno. Si no conoces la teoría exacta para defenderte con blancas, el negro te barrerá del tablero.

### 2. Gambito de Rey (`1.e4 e5 2.f4`)
- **Eval:** -0.65 (Dudoso para el blanco).
- **Win Rate:** 54% (El blanco gana más de lo que debería).
- **Conclusión:** A pesar de ser teóricamente inferior, crea un caos que favorece al blanco en partidas rápidas o entre aficionados.

## Cómo usar esta información

- **Si vas a jugar la jugada 🧪:** Úsala como un arma sorpresa. Es una línea de alto riesgo pero con una recompensa estadística probada.
- **Si tu rival puede jugar la jugada 🧪:** ¡Alerta! No te fíes de que el motor te dé ventaja. Estudia bien la línea porque lo más probable es que caigas en una trampa si improvisas.

---
*Documentación generada por fa-chess - 2026*
