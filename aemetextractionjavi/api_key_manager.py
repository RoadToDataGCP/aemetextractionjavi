import threading
import time

class APIKeyManager:
    def __init__(self, api_keys):
        self.api_keys = api_keys
        self.lock = threading.Lock()
        self.limite_por_minuto = 20
        self.uso_keys = {key: {
            "ocupada": False,
            "peticiones": 0,
            "minuto": self._minuto_actual()
        } for key in api_keys}

    def _minuto_actual(self):
        return int(time.time() // 60)

    def _reset_si_cambio_de_minuto(self, key):
        minuto_actual = self._minuto_actual()
        if self.uso_keys[key]["minuto"] != minuto_actual:
            self.uso_keys[key]["peticiones"] = 0
            self.uso_keys[key]["minuto"] = minuto_actual

    def obtener_api_key(self):
        with self.lock:
            while True:
                for key in self.api_keys:
                    self._reset_si_cambio_de_minuto(key)
                    estado = self.uso_keys[key]
                    if not estado["ocupada"] and estado["peticiones"] < self.limite_por_minuto:
                        estado["ocupada"] = True
                        estado["peticiones"] += 1
                        return key

                print("⏳ Todas las claves están ocupadas o en su límite. Esperando 5s...")
                time.sleep(5)

    def liberar_api_key(self, key):
        with self.lock:
            if key in self.uso_keys:
                self.uso_keys[key]["ocupada"] = False

    def anotar_peticion(self, key):
        with self.lock:
            self._reset_si_cambio_de_minuto(key)
            self.uso_keys[key]["peticiones"] += 1
