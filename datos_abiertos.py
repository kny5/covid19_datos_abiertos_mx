import requests
from zipfile import ZipFile
from pandas import read_csv
from os import path


# data_file = "datos_abiertos_covid19.zip"
# dicc_file_name = "diccionario_datos_covid19.zip"
url = "http://187.191.75.115/gobmx/salud/datos_abiertos/"
definitions = "Descriptores_0419.xlsx"
catalog = "Catalogos_0412.xlsx"
file_dir = ""
zip_file_list = ["datos_abiertos_covid19.zip"]


def download_zip_data_files(__url, __item):
    __zip = requests.get(str(__url + __item), allow_redirects=True)
    if open(file_dir + __item, 'wb').write(__zip.content):
        print("Guardada " + __item)
        return True
    else:
        print("Error al guardar " + __item)
        return False


def file_check(__item):
    if path.exists(__item):
        print("Existe" + __item)
        return True
    else:
        print("No existe" + __item)
        return False


def extract_zip_file(__file_name):
    print("Extrayendo ZIP")
    with ZipFile(__file_name, 'r') as zipObj:
        zipObj.extractall()
    print("CSV extraido")
    return zipObj.namelist()[0]


def filter_data_by_estado(__campo,__valor, __database):
    __filter = __database[__campo] == int(__estado)
    __filtered = __database[__filter]
    __name_file = 'out_' + str(__valor) + '.csv'
    __filtered.to_csv(__name_file, encoding='UTF-8')
    return __name_file


def main_program():
    for __item in zip_file_list:
        if file_check(__item):
            extract_zip_file(__item)
            data_file = extract_zip_file(__item)

        else:
            download_zip_data_files(url, __item)
            return main_program()

        database = read_csv(data_file, encoding="latin1")
        print("casos Mexico " + str(len(database.axes[0])))
        
        filter_data_by_estado("07", database)
        chiapas = read_csv("out_07.csv", encoding="UTF-8")
        print("casos Chiapas " + str(len(chiapas.axes[0])))
        filtered_data_by_municipio("101", chiapas)
        tuxtla = read_csv("out_101.csv", encoding="UTF-8")
        print("casos Tuxtla " + str(len(tuxtla.axes[0])))
main_program()

