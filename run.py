from library.functions import *
from datetime import datetime, timedelta

# Start time
start = time.time()

# Parámetros iniciales
url = 'https://drive.google.com/u/0/uc?export=download&confirm=4uEc&id=1NY9mapQxPGcNIsciG2WYgMJwi0Q6N9eV'  # url de descarga del .zip
datapath = 'data'  # Folder donde se almacenan lo(s) archivo(s) .zip
filename = 'products'
patternEnds = 's00.csv'  # Pattern para identificar el .csv con column names correctas

# Download .zip
download_zip_from_drive(url=url, datapath=datapath, filename=filename)

# Ciclo por si existiesen más archivos comprimidos
rutafiles = [os.path.join(datapath, f) for f in os.listdir(datapath)]
for rutafile in rutafiles:

    # Creación de directorio de trabajo
    directory = create_directory(directory=str('session' + str(datetime.now().strftime('%Y%m%d%H%M%S%f'))))

    # Extracción de data comprimida en directorio de trabajo
    time_delta = extract_data(rutafile=rutafile, directory=directory)
    print(time_delta)

    # Procesamiento de los datos
    time_delta = salesCount(directory=directory, patternEnds=patternEnds, rutafile=rutafile)
    print(time_delta)

    # Borrar directorio de trabajo
    shutil.rmtree(directory)

# Time delta proceso completo
print()
print()
print(f"Procesamiento finalizado, el tiempo total de ejecución fue de: {timedelta(seconds=time.time() - start)}")
