import pandas as pd
import json

# Cargar el archivo Excel (ajusta la ruta según tu entorno)
archivo_excel = "diccionario24.xlsx"

# Leer el archivo saltando la primera fila con encabezados incorrectos
df = pd.read_excel(archivo_excel, skiprows=1)

# Renombrar columnas para facilitar el acceso
df.columns = ['CODAUTO', 'CPRO', 'CMUN', 'DC', 'NOMBRE']

# Generar el código combinado de CPRO y CMUN, asegurando ceros a la izquierda
df['codigo'] = df['CPRO'].astype(str).str.zfill(2) + df['CMUN'].astype(str).str.zfill(3)

# Crear la lista de diccionarios para JSON
json_data = df[['NOMBRE', 'codigo']].rename(columns={'NOMBRE': 'nombre_municipio'}).to_dict(orient='records')

# Guardar como archivo JSON (opcional)
with open("municipios.json", "w", encoding="utf-8") as f:
    json.dump(json_data, f, ensure_ascii=False, indent=2)

# También puedes imprimirlo directamente si lo necesitas
print(json.dumps(json_data, ensure_ascii=False, indent=2))
