import requests
import os
import time
import logging
from dotenv import load_dotenv
from tqdm import tqdm
import datetime
import json
import threading
from utils import espera_con_barra


class AemetAPIClient:
    def __init__(self):
        load_dotenv()
        

        self.api_keys = [value for key, value in os.environ.items() if key.startswith("AEMET_API_KEY")]
        self.api_key_index = 0
        self.api_key = self.api_keys[self.api_key_index]
        self.base_url = "https://opendata.aemet.es/opendata/api"
        self.headers = {'cache-control': "no-cache"}
        self.request_count = 0

    def _cambiar_api_key(self):
        hilo = threading.current_thread().name
        anterior = self.api_key_index + 1  # +1 para numeración humana
        self.api_key_index = (self.api_key_index + 1) % len(self.api_keys)  # Vuelve al inicio al llegar al final
        self.api_key = self.api_keys[self.api_key_index]
        actual = self.api_key_index + 1
        print(f"🔁 {hilo} - Cambiando de API key {anterior} ➡️ {actual}")

    def obtener_prediccion_municipio(self, codigo_municipio, intentos=3, tiempo_espera=60):
        url = f"{self.base_url}/prediccion/especifica/municipio/diaria/{codigo_municipio}"
        for intento in range(1, intentos + 1):
            try:
                params = {"api_key": self.api_key}
                response = requests.get(url, headers=self.headers, params=params)
                self.request_count += 1
                if self.request_count >= 19:
                    self._cambiar_api_key()
                    self.request_count = 0
                if response.status_code == 200:
                    json_url = response.json().get("datos", None)
                    if json_url:
                        return self._descargar_datos_json(json_url, tiempo_espera)
                    else:
                        logging.error(f"La API no devolvió la clave 'datos'. Respuesta: {response.json()}")
                elif response.status_code == 429:
                    logging.error(f"Demasiadas solicitudes (429). Esperando {tiempo_espera} segundos...")
                    espera_con_barra(tiempo_espera, mensaje="Esperando debido a un error 429")
                elif response.status_code == 500:
                    logging.error(f"Error del servidor (500). Esperando {tiempo_espera} segundos...")
                    espera_con_barra(tiempo_espera, mensaje="Esperando debido a un error 500")
                else:
                    logging.error(f"Error inesperado ({response.status_code}): {response.text}")
            except requests.exceptions.ConnectionError:
                logging.error(f"[{codigo_municipio}] Error de conexión. Reintentando ({intento}/{intentos})...")
                espera_con_barra(tiempo_espera, mensaje="Esperando debido a un error de conexión")
            except requests.exceptions.Timeout:
                logging.error(f"[{codigo_municipio}] La solicitud tardó demasiado. Reintentando ({intento}/{intentos})...")
                espera_con_barra(tiempo_espera, mensaje="Esperando debido a un timeout")
            except requests.exceptions.RequestException as e:
                logging.error(f"[{codigo_municipio}] Error inesperado en el primer GET: {e}")
                return None
        logging.error(f"[{codigo_municipio}] No se pudo obtener predicción tras {intentos} intentos.")
        return None


    def _descargar_datos_json(self, json_url, tiempo_espera):
        """
        Descarga y devuelve los datos JSON desde la URL proporcionada, manejando errores.
        """
        try:
            response_data = requests.get(json_url, headers=self.headers)
            if response_data.status_code == 200:
                return response_data.json()
            elif response_data.status_code == 429:
                error_message = f"⚠️ Segundo GET → Demasiadas solicitudes (429). Esperando {tiempo_espera} segundos..."
                # Obtener el tiempo de reset y convertirlo
                reset_timestamp = response_data.headers.get("X-RateLimit-Reset")
                if reset_timestamp:
                    reset_time = datetime.datetime.utcfromtimestamp(int(reset_timestamp))
                    reset_time_local = reset_time.strftime('%Y-%m-%d %H:%M:%S')
                    print(f"⏱️ Reset Time (UTC): {reset_time_local} local time")
                
                print("🔁 Retry-After:", response_data.headers.get("Retry-After"))
                print("📊 Rate Limit:", response_data.headers.get("X-RateLimit-Limit"))
                print("📉 Remaining:", response_data.headers.get("X-RateLimit-Remaining"))
                print("⏱️ Reset Time:", response_data.headers.get("X-RateLimit-Reset"))
                logging.error(error_message)
                print(error_message)
                espera_con_barra(tiempo_espera, mensaje="Esperando debido a un error 429 en el segundo GET")
            else:
                error_message = f"❌ Segundo GET → Error inesperado ({response_data.status_code}): {response_data.text}"
                logging.error(error_message)
                print(error_message)
                #espera_con_barra(tiempo_espera)
        except requests.exceptions.RequestException as e:
            error_message = f"❌ Segundo GET → Excepción durante la solicitud: {e}"
            logging.error(error_message)
            print(error_message)
            #espera_con_barra(tiempo_espera)

        return None

def procesar_municipios_sin_hilos(fragmento_municipios, api_keys):
    cliente = AemetAPIClient()
    cliente.api_keys = api_keys
    cliente.api_key = api_keys[0]
    cliente.api_key_index = 0
    cliente.request_count = 0

    predicciones_municipios = []
    municipios_fallidos = []

    for idx, municipio in enumerate(fragmento_municipios, start=1):
        codigo = municipio["codigo_municipio"]
        nombre = municipio.get("NOMBRE", "Desconocido")
        print(f"Procesando {idx}/{len(fragmento_municipios)}: {codigo} ({nombre})")

        for intento in range(3):
            try:
                prediccion = cliente.obtener_prediccion_municipio(codigo)
                if prediccion:
                    for dia in prediccion:
                        dia.pop("origen", None)
                    predicciones_municipios.append({
                        "codigo_municipio": codigo,
                        "nombre": nombre,
                        "prediccion": prediccion
                    })
                    break
                elif intento == 2:
                    municipios_fallidos.append({"codigo_municipio": codigo, "nombre": nombre})
            except Exception as e:
                if intento == 2:
                    municipios_fallidos.append({"codigo_municipio": codigo, "nombre": nombre})
                    logging.error(f"Error crítico procesando {codigo} ({nombre}): {e}")

    return predicciones_municipios, municipios_fallidos
