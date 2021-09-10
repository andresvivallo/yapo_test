import zipfile
import time
from datetime import timedelta
import os
import shutil
import pandas as pd
import json
import gdown


def create_directory(directory=None):
    """
    Check and create directory

    Parameters
    ----------
    directory : str
        Folder directory

    Returns
    -------
    Create a directory folder and return str
    """
    full_path = os.path.join(os.getcwd(), directory)
    if not os.path.exists(full_path):
        os.makedirs(full_path)
    else:
        shutil.rmtree(full_path)
        os.makedirs(full_path)

    return directory


def extract_data(rutafile=None, directory=None):
    """
    Extract files from file .zip

    Parameters
    ----------
    rutafile : str
        The file location of .zip
    directory : str
        The folder location where extract the files

    Returns
    -------
    Extract files in a directory folder
    """

    # time start
    start = time.time()

    with zipfile.ZipFile(rutafile, 'r') as zip_ref:
        print()
        print('Extrayendo archivos desde zip, esto puede tardar unos minutos...')
        zip_ref.extractall(directory)

        segundos = time.time() - start

    return f'Extracción de archivos finalizada, tiempo de ejecución: {timedelta(seconds=segundos)}'


def extract_headers(files=None, patternEnds=None):
    """
    Extract headers from first csv of the month

    Parameters
    ----------
    files : list
        list of files .csv of a specific month
    patternEnds : str
        str endwith the file with de columns names correct

    Returns
    -------
    headers: list
        list of headers of .csv files
    """

    for file in files:
        if os.path.split(file)[-1].endswith(patternEnds):
            df = pd.read_csv(file)
            headers = list(df.columns)
        else:
            continue

    return headers


def salesCount(directory=None, patternEnds=None, rutafile=None):
    """
    Calculate sales count groupby date and product_name

    Parameters
    ----------
    directory : str
        Path directory of temporal files
    patternEnds : str
        str endwith the file with de columns names correct
    rutafile: str
        The file location of .zip

    Returns
    -------
    count_result: json
        json with the results of sales count group by date and product_name
    """

    start = time.time()
    conteo_consolidado_list = []

    months = [os.path.join(directory, f) for f in os.listdir(directory)]
    for month in months:
        print()
        print()
        print('Procesando mes: ' + '\033[1m' + str(os.path.split(month)[-1]) + '\033[0m' + ', esto tomará unos minutos')
        print()
        files = [os.path.join(month, f) for f in os.listdir(month)]
        files.sort()

        # Extraer nombres de columnas o headers desde el endwith('s00.csv')
        headers = extract_headers(files=files, patternEnds=patternEnds)

        # Procesamiento de cada archivo
        for file in files:
            print('Calculando estadística de file: ' + '\033[1m' + str(os.path.split(file)[-1]) + '\033[0m')
            if os.path.split(file)[-1].endswith(patternEnds):
                df = pd.read_csv(file)
            else:
                df = pd.read_csv(file, names=headers)

            # Filtrar aquellos clientes PRO
            df.pri_pro = df.pri_pro.str.lower()
            is_PRO = df.pri_pro == 'pro'
            df_PRO = df[is_PRO]

            # Conteo de venta de productos por día por archivo por separado
            tmp = df_PRO.groupby(['product_name', 'creation_date']).size().reset_index(name='count')
            conteo_consolidado_list.append(tmp)

    # Concatenar el listado del conteo en un unico DataFrame
    df_conteo_consolidado = pd.concat(conteo_consolidado_list)

    # Agrupación de conteos en el dataFrame consolidado
    df_conteo_consolidado_2 = df_conteo_consolidado.\
        groupby(['product_name', 'creation_date']).\
        agg({'count': 'sum'}).reset_index()

    # Tabla larga a tabla ancha
    wide_conteo = df_conteo_consolidado_2.pivot(index='product_name', columns='creation_date', values='count')

    # Transformar y guardar como archivo .json
    jsontmp = json.loads(wide_conteo.to_json(orient='index'))
    with open(str(os.path.split(rutafile)[-1]).replace('.zip', '') + '_final_results.json', 'w', encoding='utf-8') as f:
        json.dump(jsontmp, f, ensure_ascii=False, indent=4)

    print()
    return f'Conteo de ventas finalizado, tiempo de ejecución: {timedelta(seconds=time.time() - start)}'


def download_zip_from_drive(url=None, datapath=None, filename=None):
    """
   Download .zip from drive url and save with specific location and name

   Parameters
   ----------
   url : str
       web url direction of .zip download
   filename : str
       name of download file
   datapath: str
       folder location of .zip

   Returns
   -------
   Download zip from drive url
   """

    full_path = os.path.join(os.getcwd(), datapath)
    if not os.path.exists(full_path):
        os.makedirs(full_path)
    else:
        shutil.rmtree(full_path)
        os.makedirs(full_path)
    output = os.path.join(full_path, filename + '.zip')

    print()
    print('Comenzando descarga de archivo .zip')
    gdown.download(url, output, quiet=False)
    print('Descarga finalizada')
