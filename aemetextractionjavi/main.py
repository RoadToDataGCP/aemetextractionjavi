from main_menu import cargar_municipios, cargar_predicciones, formato_hms, limpiar_archivos_generados, subir_a_bucket
import time

def main():
    limpiar_archivos_generados()
    hora_inicio = time.time()
    print("🔄 Iniciando proceso completo...")
    cargar_municipios()
    carga_municipios=time.time()
    cargar_predicciones()
    carga_predicciones=time.time()
    hora_fin = time.time()
    print("🔄 Proceso completo")
    duracion_municipios = carga_municipios - hora_inicio
    duracion_predicciones = carga_predicciones - carga_municipios
    print(f"⏱️ Carga de municipios: {formato_hms(duracion_municipios)}")
    print(f"⏱️ Carga de predicciones: {formato_hms(duracion_predicciones)}")
    duracion = hora_fin - hora_inicio
    print(f"⏱️ Duración total del proceso: {formato_hms(duracion)}")
    print("🔄 Subiendo archivos a bucket...")
    subir_a_bucket()
    print("🔄 Archivos subidos a bucket")

if __name__ == "__main__":
    main()
