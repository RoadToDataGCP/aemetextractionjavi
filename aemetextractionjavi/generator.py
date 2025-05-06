import json
import csv
import datetime
import os

def create_csv_files_from_json(json_file):
    # Cargar los datos JSON desde el archivo
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Obtener el timestamp actual para ts_insert y ts_update
    ts_insert = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ts_update = ts_insert

    # Crear temperatura_media.csv
    with open('temperatura_media.csv', 'w', newline='', encoding='utf-8') as temp_file:
        temp_writer = csv.writer(temp_file, delimiter=';')
        temp_writer.writerow(['Fecha', 'Codigo_municipio', 'Provincia', 'Municipio', 'tmed', 'ts_insert', 'ts_update'])
        for idema, records in data.items():
            for record in records:
                temp_writer.writerow([record.get('fecha'), idema, record.get('provincia'), record.get('nombre'), record.get('tmed'), ts_insert, ts_update])

    # Crear precipitaciones.csv
    with open('precipitaciones.csv', 'w', newline='', encoding='utf-8') as prec_file:
        prec_writer = csv.writer(prec_file, delimiter=';')
        prec_writer.writerow(['Fecha', 'Codigo_municipio', 'Provincia', 'Municipio', 'prec', 'ts_insert', 'ts_update'])
        for idema, records in data.items():
            for record in records:
                prec_writer.writerow([record.get('fecha'), idema, record.get('provincia'), record.get('nombre'), record.get('prec'), ts_insert, ts_update])

    # Crear tiempo_max_min.csv
    with open('tiempo_max_min.csv', 'w', newline='', encoding='utf-8') as atm_file:
        atm_writer = csv.writer(atm_file, delimiter=';')
        atm_writer.writerow(['Fecha', 'Codigo_municipio', 'Provincia', 'Municipio', 'Hora', 'tmin', 'horatmin', 'tmax', 'horatmax', 'ts_insert', 'ts_update'])
        for idema, records in data.items():
            for record in records:
                atm_writer.writerow([record.get('fecha'), idema, record.get('provincia'), record.get('nombre'), record.get('horatmin'), record.get('tmin'), record.get('horatmin'), record.get('tmax'), record.get('horatmax'), ts_insert, ts_update])

    print("Archivos CSV creados exitosamente.")

# Llamada a la función con el archivo JSON generado
if os.path.exists("registros_meteorologicos.json"):
    create_csv_files_from_json('registros_meteorologicos.json')

def json_to_csv_historic(input_file, output_file):
    # Abrir el archivo JSON para leer los datos
    with open(input_file, 'r', encoding='utf-8') as file:
        json_data = json.load(file)
    
    # Obtener las claves para el encabezado del CSV (esto es consistente para todos los registros)
    headers = ["fecha", "indicativo", "nombre", "provincia", "altitud", "tmed", "prec", "tmin", "horatmin", "tmax", "horatmax"]
    
    # Abrir el archivo CSV para escritura
    with open(output_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        
        # Escribir el encabezado
        writer.writeheader()
        
        # Recorrer los datos del JSON
        for key in json_data:
            for entry in json_data[key]:
                # Escribir cada entrada del JSON como una fila en el CSV
                writer.writerow(entry)

def json_to_csv_predict(input_file, output_file):
    # Abrir el archivo JSON para leer los datos
    with open(input_file, 'r', encoding='utf-8') as file:
        json_data = json.load(file)
    
    # Definir el encabezado del CSV
    headers = [
        "codigo_municipio", "nombre_municipio", "elaborado", "provincia", 
        "fecha", "periodo", "probPrecipitacion", "cotaNieveProv", "estadoCielo", 
        "viento_direccion", "viento_velocidad", "rachaMax", "temperatura_maxima", 
        "temperatura_minima", "temperatura_hora", "temperatura_value", 
        "sensTermica_maxima", "sensTermica_minima", "sensTermica_hora", "sensTermica_value", 
        "humedadRelativa_maxima", "humedadRelativa_minima", "humedadRelativa_hora", 
        "humedadRelativa_value", "uvMax"
    ]
    
    # Abrir el archivo CSV para escritura
    with open(output_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()

        # Recorrer los municipios en el JSON
        for municipio in json_data:
            codigo_municipio = municipio["codigo_municipio"]
            nombre_municipio = municipio["nombre"]
            
            # Recorrer las predicciones
            for prediccion in municipio["prediccion"]:
                elaborado = prediccion["elaborado"]
                provincia = prediccion["provincia"]
                
                # Recorrer los días de predicción
                for dia in prediccion["prediccion"]["dia"]:
                    # Desanidar los datos de cada día
                    fecha = dia["fecha"]
                    
                    for i in range(len(dia["probPrecipitacion"])):
                        if i < len(dia["temperatura"]["dato"]) and i < len(dia["sensTermica"]["dato"]) and i < len(dia["humedadRelativa"]["dato"]):

                            row = {
                                "codigo_municipio": codigo_municipio,
                                "nombre_municipio": nombre_municipio,
                                "elaborado": elaborado,
                                "provincia": provincia,
                                "fecha": fecha,
                                "periodo": dia["probPrecipitacion"][i]["periodo"],
                                "probPrecipitacion": dia["probPrecipitacion"][i]["value"],
                                "cotaNieveProv": dia["cotaNieveProv"][i]["value"],
                                "estadoCielo": dia["estadoCielo"][i]["descripcion"],
                                "viento_direccion": dia["viento"][i]["direccion"],
                                "viento_velocidad": dia["viento"][i]["velocidad"],
                                "rachaMax": dia["rachaMax"][i]["value"],
                                "temperatura_maxima": dia["temperatura"]["maxima"],
                                "temperatura_minima": dia["temperatura"]["minima"],
                                "temperatura_hora": dia["temperatura"]["dato"][i]["hora"],
                                "temperatura_value": dia["temperatura"]["dato"][i]["value"],
                                "sensTermica_maxima": dia["sensTermica"]["maxima"],
                                "sensTermica_minima": dia["sensTermica"]["minima"],
                                "sensTermica_hora": dia["sensTermica"]["dato"][i]["hora"],
                                "sensTermica_value": dia["sensTermica"]["dato"][i]["value"],
                                "humedadRelativa_maxima": dia["humedadRelativa"]["maxima"],
                                "humedadRelativa_minima": dia["humedadRelativa"]["minima"],
                                "humedadRelativa_hora": dia["humedadRelativa"]["dato"][i]["hora"],
                                "humedadRelativa_value": dia["humedadRelativa"]["dato"][i]["value"],
                                "uvMax": dia["uvMax"]
                            }
                            writer.writerow(row)




