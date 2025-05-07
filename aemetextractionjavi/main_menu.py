
from connection import AemetAPIClient, procesar_municipios_sin_hilos
import pandas as pd
import datetime
import logging
import time
import json
import csv
import os
from google.cloud import storage
from google.cloud import bigquery

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
    #subir_a_bucket(final_file, "aemetextractionjavi")
    automatizar_carga_bigquery(
    csv_path=f"{final_file}",
    project_id="r2d-interno-dev",
    dataset_id="raw_aemet",
    table_id="aemetextractionjavi_raw"
)

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

    print(f"🔄 Archivos subidos a gs://{bucket_name}/{destination_blob_name}")

def verificar_csv_no_vacio(csv_path):
    if os.path.isfile(csv_path) and os.path.getsize(csv_path) > 0:
        logging.info(f"📄 El CSV '{csv_path}' existe y no está vacío.")
        return True
    else:
        logging.error(f"❌ El archivo CSV '{csv_path}' está vacío o no existe.")
        return False

def tabla_existe(client, project_id, dataset_id, table_id):
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    try:
        client.get_table(table_ref)
        logging.info(f"📊 La tabla '{table_ref}' existe en BigQuery.")
        return True
    except Exception as e:
        logging.error(f"❌ No se encontró la tabla '{table_ref}': {e}")
        return False

def borrar_datos_tabla(client, project_id, dataset_id, table_id):
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    query = f"DELETE FROM `{table_ref}` WHERE TRUE"
    client.query(query).result()
    logging.info(f"🗑️ Se han borrado los datos existentes en la tabla '{table_ref}'.")

def cargar_csv_a_bigquery(client, csv_path, project_id, dataset_id, table_id):
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=False,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema_update_options=[],
        field_delimiter=",",
        quote_character='"',
    )

    # Leer los encabezados del CSV y reemplazar guiones por guiones bajos
    with open(csv_path, 'r', encoding='utf-8') as f:
        headers = f.readline().strip().split(',')
    
    # Reemplazar los guiones por guiones bajos en los nombres de las columnas
    headers = [header.replace('-', '_') for header in headers]

    # Añadir columna con la fecha de carga
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    
    # Crear un archivo temporal con la columna 'fecha_carga' añadida
    tmp_path = "tmp_bq_upload.csv"
    with open(tmp_path, 'w', encoding='utf-8') as fout:
        with open(csv_path, 'r', encoding='utf-8') as fin:
            for i, line in enumerate(fin):
                line = line.strip()
                if i == 0:
                    fout.write(f"{','.join(headers)}\n")
                else:
                    fout.write(f"{line},{today}\n")

    # Subir el archivo CSV modificado a BigQuery
    with open(tmp_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
        job.result()

    # Eliminar el archivo temporal
    os.remove(tmp_path)

    logging.info(f"✅ Carga completada: {len(headers)} columnas + campo 'fecha_carga' en '{table_ref}'.")


def automatizar_carga_bigquery(csv_path, project_id, dataset_id, table_id):
    logging.info("🚀 Iniciando proceso de carga de datos a BigQuery...")

    if not verificar_csv_no_vacio(csv_path):
        return

    client = bigquery.Client()

    if not tabla_existe(client, project_id, dataset_id, table_id):
        return

    borrar_datos_tabla(client, project_id, dataset_id, table_id)
    cargar_csv_a_bigquery(client, csv_path, project_id, dataset_id, table_id)

    logging.info("🎯 Proceso finalizado correctamente.")
