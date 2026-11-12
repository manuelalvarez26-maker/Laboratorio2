import hashlib
import pandas as pd

ORIGINAL_FILE = "datos/telemetria_envios.csv"
CLEAN_FILE = "datos/telemetria_limpia.csv"

def obtener_sha256(ruta_fichero):
    sha = hashlib.sha256()
    with open(ruta_fichero, "rb") as f:
        for bloque in iter(lambda: f.read(4096), b""):
            sha.update(bloque)
    return sha.hexdigest()

def normalizar_datos():
    print(f"--- Calculando SHA-256 Inicial ---")
    hash_ini = obtener_sha256(ORIGINAL_FILE)
    print(f"Hash Original: {hash_ini}\n")

    df = pd.read_csv(ORIGINAL_FILE)
    filas_iniciales = len(df)

    # 1. Eliminación de duplicados
    df = df.drop_duplicates()

    # 2. Eliminación de nulos
    df = df.dropna()

    # 3. Filtrado de pesajes válidos (peso > 0)
    df = df[df['peso_kg'] > 0]

    # 4. Filtrado de temperaturas razonables (-30°C a 40°C)
    df = df[(df['temp_celsius'] >= -30) & (df['temp_celsius'] <= 40)]

    # 5. Filtrado de estados válidos del catálogo
    estados_validos = ['ENTREGADO', 'EN_TRANSITO', 'EN_REPARTO', 'INCIDENCIA_REPARTO']
    df = df[df['estado_envio'].isin(estados_validos)]

    # Guardar dataset saneado
    df.to_csv(CLEAN_FILE, index=False)
    filas_finales = len(df)

    print(f"--- Proceso de Depuración ---")
    print(f"Filas originales: {filas_iniciales}")
    print(f"Filas depuradas:  {filas_finales}")
    print(f"Registros descartados: {filas_iniciales - filas_finales}")

    hash_fin = obtener_sha256(CLEAN_FILE)
    print(f"\n--- Calculando SHA-256 Saneado ---")
    print(f"Hash Limpio:   {hash_fin}\n")

if __name__ == "__main__":
    normalizar_datos()