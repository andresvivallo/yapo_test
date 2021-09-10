from library.functions import *
from datetime import datetime
start = time.time()

# Parámetros iniciales
rutafile = 'data/products.zip'  # Ruta de archivo .zip
directory = str('session' + str(datetime.now().strftime('%Y%m%d%H%M%S%f')))
directory = 'session20210910015759212380'
patternEnds = 's00.csv'  # Pattern con que termina el .csv con column names correctas

# Creación de directorio de trabajo
create_directory(directory=directory)

# Extracción de data comprimida en directorio de trabajo
time_delta = extract_data(rutafile=rutafile, directory=directory)
print(time_delta)

# Procesamiento de los datos
time_delta = salesCount(directory=directory, patternEnds=patternEnds)
print(time_delta)

# shutil.rmtree(directory)

print()
print()
print(f"Todo el proceso demoró: {timedelta(seconds=time.time() - start)}")
