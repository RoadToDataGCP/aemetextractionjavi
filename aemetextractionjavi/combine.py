import json
import unicodedata
import pandas as pd
import os


def normalizar(nombre: str) -> str:
    nfkd = unicodedata.normalize('NFKD', nombre)
    return ''.join([c for c in nfkd if not unicodedata.combining(c)]).upper()


def combinar_jsons(
    meteo_data,
    municipios_path,
    output_json=None,
    output_csv=None
):
    # Cargar los JSONs
    with open(municipios_path, "r", encoding="utf-8") as f:
        municipios = json.load(f)

    # Crear mapa de nombre normalizado -> código
    mapa_codigos = {
        normalizar(m["nombre"]): m["codigo_municipio"]
        for m in municipios
    }

    # Añadir código a cada entrada meteorológica
    for entry in meteo_data:
        nombre_norm = normalizar(entry["nombre"])
        entry["codigo"] = mapa_codigos.get(nombre_norm, None)

    if not os.path.exists(os.path.dirname(output_json)):
        os.makedirs(os.path.dirname(output_json))

    # Guardar como JSON si se especifica
    if output_json:
        with open(output_json, "w", encoding="utf-8") as f:
            json.dump(meteo_data, f, ensure_ascii=False, indent=2)

    # Guardar como CSV si se especifica
    if output_csv:
        df = pd.DataFrame(meteo_data)
        df.to_csv(output_csv, index=False, encoding="utf-8")

    return meteo_data

