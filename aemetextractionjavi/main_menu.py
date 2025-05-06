
from connection import AemetAPIClient, procesar_municipios_sin_hilos
from generator import create_csv_files_from_json, json_to_csv_historic, json_to_csv_predict
import pandas as pd
import datetime
import logging
import time
import json
import csv
import os
from google.cloud import storage

def cargar_estaciones():
    client = AemetAPIClient()
    print("📡 Obteniendo estaciones disponibles...")
    estaciones = client.obtener_estaciones()

    if not estaciones:
        print("❌ No se pudieron obtener las estaciones.")
        return

    estaciones_json = []
    for estacion in estaciones:
        idema = estacion.get("indicativo")
        nombre = estacion.get("nombre")
        if idema and nombre:
            estaciones_json.append({"idema": idema, "nombre": nombre})

    with open("estaciones.json", "w", encoding="utf-8") as f:
        json.dump(estaciones_json, f, indent=4, ensure_ascii=False)

    print("✅ Listado de estaciones guardado en 'estaciones.json'.")

def cargar_municipios():

    # Cargar el archivo Excel, saltando la primera fila para obtener los encabezados correctos
    df = pd.read_excel('diccionario24.xlsx', engine='openpyxl', skiprows=1)

    # Combinar cpro y cmun para crear el código del municipio, manteniendo los ceros a la izquierda
    df['codigo_municipio'] = df['CPRO'].astype(str).str.zfill(2) + df['CMUN'].astype(str).str.zfill(3)

    # Extraer las columnas requeridas: 'codigo_municipio' y 'NOMBRE'
    municipio_df = df[['codigo_municipio', 'NOMBRE']]

    # Crear una lista de diccionarios con el codigo_municipio y nombre
    municipio_list = municipio_df.to_dict(orient='records')

    # Guardar la lista en un archivo JSON
    with open('municipios.json', 'w', encoding='utf-8') as f:
        json.dump(municipio_list, f, indent=4, ensure_ascii=False)

    print("Archivo JSON 'municipios.json' creado exitosamente.")

# Función para cargar predicciones con manejo de errores, reintentos, logging y limitación de tasa
def cargar_predicciones():
    logging.basicConfig(level=logging.ERROR,
                        format='%(asctime)s - %(levelname)s - %(message)s')

    client = AemetAPIClient()

    with open("municipios.json", "r", encoding="utf-8") as f:
        municipios = json.load(f)

    municipios = municipios[:10]  # Solo los primeros 10

    predicciones, fallidos = procesar_municipios_sin_hilos(municipios, client.api_keys)

    fecha = datetime.datetime.now().strftime("%Y-%m-%d")
    output_file = f"predicciones_municipios_{fecha}.json"
    final_file = f"predicciones_municipios_{fecha}.csv"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(predicciones, f, indent=4, ensure_ascii=False)

    print(f"\n✅ Municipios procesados correctamente: {len(predicciones)}")
    print(f"❌ Municipios con error después de reintentos: {len(fallidos)}")
    if fallidos:
        for municipio in fallidos:
            print(f" - {municipio['codigo_municipio']} ({municipio['nombre']})")
    print(f"✅ Predicciones guardadas en '{output_file}'")

    convertir_json_a_csv(output_file, final_file)
    print(f"✅ CSV de predicciones por municipio creado en '{final_file}'")
    subir_a_bucket(final_file, "aemetextractionjavi")
    print("🔄 Archivos subidos a bucket")


def limpiar_archivos_generados():
    print("🧹 Limpiando archivos generados de ejecuciones anteriores...")
    archivos_a_borrar = [
        "estaciones.json",
        "municipios.json",
        "registros_meteorologicos.json",
    ]

    # Borrar archivos individuales si existen
    for archivo in archivos_a_borrar:
        if os.path.exists(archivo):
            os.remove(archivo)
            print(f"🗑️ Archivo eliminado: {archivo}")

    # Borrar archivos temporales por patrón
    patrones = [
        "historico_hilo_",
        "predicciones_municipios_hilo_",
        "predicciones_municipios_"
    ]
    for nombre_archivo in os.listdir():
        for patron in patrones:
            if nombre_archivo.startswith(patron) and nombre_archivo.endswith(".json"):
                if os.path.exists(nombre_archivo):
                    os.remove(nombre_archivo)
                    print(f"🗑️ Archivo eliminado: {nombre_archivo}")

    # Limpiar carpeta output/
    if os.path.exists("output"):
        for archivo in os.listdir("output"):
            ruta = os.path.join("output", archivo)
            if os.path.isfile(ruta):
                os.remove(ruta)
                print(f"🗑️ Archivo eliminado: {ruta}")

def formato_hms(segundos):
    return time.strftime("%H:%M:%S", time.gmtime(segundos))


def main():
    
    while True:
        print("\n📋 ¿Qué acción deseas realizar?")
        print("1️⃣  Cargar datos de estaciones")
        print("2️⃣  Cargar datos de municipios")
        print("4️⃣  Cargar predicciones por municipio")
        print("6️⃣  Proceso completo")
        print("7️⃣  Limpiar archivos generados")
        print("0️⃣  Salir")

        opcion = input("👉 Ingresa una opción: ").strip()

        if opcion == "1":
            cargar_estaciones()
        elif opcion == "2":
            cargar_municipios()
        elif opcion == "4":
            cargar_predicciones()
        elif opcion == "6":
            hora_inicio = time.time()
            print("🔄 Iniciando proceso completo...")
            cargar_estaciones()
            carga_estaciones=time.time()
            cargar_municipios()
            carga_municipios=time.time()
            carga_historico=time.time()
            cargar_predicciones()
            carga_predicciones=time.time()
            hora_fin = time.time()
            print("🔄 Proceso completo")
            duracion_estaciones = carga_estaciones - hora_inicio
            duracion_municipios = carga_municipios - carga_estaciones
            duracion_historico = carga_historico - carga_municipios
            duracion_predicciones = carga_predicciones - carga_historico
            duracion_combinacion = hora_fin - carga_predicciones
            print(f"⏱️ Carga de estaciones: {formato_hms(duracion_estaciones)}")
            print(f"⏱️ Carga de municipios: {formato_hms(duracion_municipios)}")
            print(f"⏱️ Carga de histórico: {formato_hms(duracion_historico)}")
            print(f"⏱️ Carga de predicciones: {formato_hms(duracion_predicciones)}")
            print(f"⏱️ Combinación de datos: {formato_hms(duracion_combinacion)}")
            duracion = hora_fin - hora_inicio
            print(f"⏱️ Duración total del proceso: {formato_hms(duracion)}")
        elif opcion == "7":
            limpiar_archivos_generados()
            print("✅ Archivos generados eliminados.")
        elif opcion == "0":
            print("👋 Saliendo del programa.")
            break
        else:
            print("❌ Opción inválida. Intenta de nuevo.")

def convertir_json_a_csv(json_file, csv_file):
    # Cargar los datos JSON desde el archivo con la codificación correcta
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Obtener la fecha de hoy en el formato requerido
    today_date = datetime.datetime.now().strftime("%Y-%m-%dT00:00:00")

    # Preparar el archivo CSV
    with open(csv_file, 'w', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile)

        # Escribir la fila de encabezado
        header = ['codigo_municipio', 'nombre', 'provincia', 'fecha', 
                  'probPrecipitacion_00-24', 'probPrecipitacion_00-12', 'probPrecipitacion_12-24', 'probPrecipitacion_00-06', 'probPrecipitacion_06-12', 'probPrecipitacion_12-18', 'probPrecipitacion_18-24',
                  'cotaNieveProv_00-24', 'cotaNieveProv_00-12', 'cotaNieveProv_12-24', 'cotaNieveProv_00-06', 'cotaNieveProv_06-12', 'cotaNieveProv_12-18', 'cotaNieveProv_18-24',
                  'estadoCielo_00-24', 'estadoCielo_00-12', 'estadoCielo_12-24', 'estadoCielo_00-06', 'estadoCielo_06-12', 'estadoCielo_12-18', 'estadoCielo_18-24',
                  'viento_direccion_00-24', 'viento_velocidad_00-24',
                  'viento_direccion_00-12', 'viento_velocidad_00-12',
                  'viento_direccion_12-24', 'viento_velocidad_12-24',
                  'viento_direccion_00-06', 'viento_velocidad_00-06',
                  'viento_direccion_06-12', 'viento_velocidad_06-12',
                  'viento_direccion_12-18', 'viento_velocidad_12-18',
                  'viento_direccion_18-24', 'viento_velocidad_18-24',
                  'rachaMax_00-24', 'rachaMax_00-12', 'rachaMax_12-24',
                  'rachaMax_00-06', 'rachaMax_06-12', 'rachaMax_12-18','rachaMax_18-24',
                  'temperatura_maxima', 'temperatura_minima',
                  'sensTermica_maxima', 'sensTermica_minima',
                  'humedadRelativa_maxima','humedadRelativa_minima',
                  'uvMax']
        csv_writer.writerow(header)

        # Iterar a través de cada municipio y filtrar los datos para hoy
        for municipio in data:
            codigo_municipio = municipio['codigo_municipio']
            nombre = municipio['nombre']
            provincia = municipio['prediccion'][0]['provincia']
            
            for dia in municipio['prediccion'][0]['prediccion']['dia']:
                if dia['fecha'] == today_date:
                    probPrecipitacion = [periodo['value'] if periodo.get('value') else 'null' for periodo in dia['probPrecipitacion']]
                    cotaNieveProv = [periodo['value'] if periodo.get('value') else 'null' for periodo in dia['cotaNieveProv']]
                    estadoCielo = [periodo['descripcion'] if periodo.get('descripcion') else 'null' for periodo in dia['estadoCielo']]
                    viento_direccion = [periodo['direccion'] if periodo.get('direccion') else 'null' for periodo in dia['viento']]
                    viento_velocidad = [periodo['velocidad'] if periodo.get('velocidad') else 'null' for periodo in dia['viento']]
                    rachaMax = [periodo['value'] if periodo.get('value') else 'null' for periodo in dia['rachaMax']]
                    temperatura_maxima = dia['temperatura']['maxima'] if dia['temperatura'].get('maxima') else 'null'
                    temperatura_minima = dia['temperatura']['minima'] if dia['temperatura'].get('minima') else 'null'
                    sensTermica_maxima = dia['sensTermica']['maxima'] if dia['sensTermica'].get('maxima') else 'null'
                    sensTermica_minima = dia['sensTermica']['minima'] if dia['sensTermica'].get('minima') else 'null'
                    humedadRelativa_maxima = dia['humedadRelativa']['maxima'] if dia['humedadRelativa'].get('maxima') else 'null'
                    humedadRelativa_minima = dia['humedadRelativa']['minima'] if dia['humedadRelativa'].get('minima') else 'null'
                    uvMax = dia.get('uvMax', 'null')

                    # Asegurarse de que todas las listas tengan la longitud correcta (7 elementos)
                    probPrecipitacion += ['null'] * (7 - len(probPrecipitacion))
                    cotaNieveProv += ['null'] * (7 - len(cotaNieveProv))
                    estadoCielo += ['null'] * (7 - len(estadoCielo))
                    viento_direccion += ['null'] * (7 - len(viento_direccion))
                    viento_velocidad += ['null'] * (7 - len(viento_velocidad))
                    rachaMax += ['null'] * (7 - len(rachaMax))

                    # Escribir la fila en el archivo CSV
                    csv_writer.writerow([codigo_municipio, nombre, provincia, today_date] + 
                                        probPrecipitacion + cotaNieveProv + estadoCielo + 
                                        viento_direccion + viento_velocidad + rachaMax + 
                                        [temperatura_maxima, temperatura_minima, sensTermica_maxima, sensTermica_minima, humedadRelativa_maxima, humedadRelativa_minima, uvMax])

    print("El archivo JSON se ha convertido a CSV y se ha filtrado para el día actual.")



def subir_a_bucket(csv_file_local, bucket_name):
    today_date = datetime.datetime.now().strftime("%Y-%m-%d")
    destination_blob_name = f"output/{today_date}/{csv_file_local.split('/')[-1]}"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(csv_file_local)

    print(f"Archivo subido a gs://{bucket_name}/{destination_blob_name}")
if __name__ == "__main__":
    main()
