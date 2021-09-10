import zipfile
import time
from datetime import timedelta
import os
import shutil
import pandas as pd


def create_directory(directory=None):
    """
    Check and create directory

    Parameters
    ----------
    directory : str
        Folder directory

    Returns
    -------
    Create a directory folder
    """

    if not os.path.exists(os.getcwd() + '/' + directory + '/'):
        os.makedirs(os.getcwd() + '/' + directory + '/')
    else:
        shutil.rmtree(os.getcwd() + '/' + directory + '/')
        os.makedirs(os.getcwd() + '/' + directory + '/')


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
        print("Extrayendo archivos, esto puede tardar unos minutos...")
        zip_ref.extractall(directory)

        segundos = time.time() - start

    return f"Extracción de archivos finalizada, tiempo de ejecución: {timedelta(seconds=segundos)}"


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
        if file.split('/')[-1].endswith(patternEnds):
            df = pd.read_csv(file)
            headers = list(df.columns)
        else:
            continue

    return headers


def salesCount(directory=None, patternEnds=None):
    #TODO: editar esta parte
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

    start = time.time()
    conteo_consolidado_list = []

    months = [os.path.join(directory, f) for f in os.listdir(directory)]
    for month in months:
        print()
        print()
        print("Procesando mes:", month, "esto tomará unos minutos")
        files = [os.path.join(month, f) for f in os.listdir(month)]
        files.sort()

        # Extraer nombres de columnas o headers desde el endwith('s00.csv')
        headers = extract_headers(files=files, patternEnds=patternEnds)

        # Procesamiento de cada archivo
        for file in files:
            print("Procesando:", file)
            if file.split('/')[-1].endswith(patternEnds):
                df = pd.read_csv(file)
            else:
                df = pd.read_csv(file, names=headers)

            # Filtrar aquellos clientes PRO
            df.pri_pro = df.pri_pro.str.lower()
            is_PRO = df.pri_pro == 'pro'
            df_PRO = df[is_PRO]

            # Conteo de venta de productos por día
            tmp = df_PRO.groupby(['product_name', 'creation_date']).size().reset_index(name='count')
            conteo_consolidado_list.append(tmp)

    # Concatenar el listado del conteo en un unico DataFrame
    df_conteo_consolidado = pd.concat(conteo_consolidado_list)
    df_conteo_consolidado.to_csv(directory + '/conteo_consolidado.csv', index=False)

    print()
    print()

    return f"Conteo de ventas finalizado, tiempo del proceso: {timedelta(seconds=time.time() - start)}"
