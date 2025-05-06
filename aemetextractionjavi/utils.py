import time
import threading
from tqdm import tqdm

def espera_con_barra(segundos: int, mensaje: str = "Esperando"):
    hilo = threading.current_thread().name
    for _ in tqdm(range(segundos), desc=f"⏳ {mensaje} - {hilo}", ncols=150):
        time.sleep(1)
