import requests
from zipfile import ZipFile
from pandas import read_csv, read_excel
from os import path, listdir, getcwd

main_dir = getcwd()
# data_file = ""
# dicc_file_name = "diccionario_datos_covid19.zip"
url = "http://187.191.75.115/gobmx/salud/datos_abiertos/"
definitions = "Descriptores_0419.xlsx"
catalog = "diccionario_datos_covid19/Catalogos_0412.xlsx"
file_dir = ""
zip_file_list = ["datos_abiertos_covid19.zip", "diccionario_datos_covid19.zip"]


def search_on_catalog(__entidad, __municipio, __index):
    __filter_ent = __index['CLAVE_ENTIDAD'] == int(__entidad)
    __filtered_ent = __index[__filter_ent]
    if not __municipio:
        return __filtered_ent
    else:
        __filter_mun = __index['CLAVE_MUNICIPIO'] == int(__municipio)
        __filtered_mun = __index[__filter_mun]
        _filter_ent = __filtered_mun['CLAVE_ENTIDAD'] == int(__entidad)
        return __filtered_mun[__filter_ent]

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
        print("Existe " + __item)
        return True
    else:
        print("No existe " + __item)
        return False


def extract_zip_file(__file_name):
    print("Extrayendo ZIP")
    with ZipFile(__file_name, 'r') as zipObj:
        zipObj.extractall()
    print("CSV extraido")
    return zipObj.namelist()[0]


def filter_data_by(__campo,__valor, __database, __type, __delta):
    # print(search_on_catalog(__valor, index))
    __filter = __database[__campo] == __type(__valor)
    __filtered = __database[__filter]
    __name_file = 'out_' + str(__campo) + '_' + str(__valor) + '.csv'
    __filtered.to_csv(__name_file, encoding='UTF-8')
    if __delta:
        print(str(__campo) + "_" + str(__valor) + " == " + str((len(__database.axes[0]) - len(__filtered.axes[0]))))
    else:
        print(str(__campo) + "_" + str(__valor) + " == " + str(len(__filtered.axes[0])))
    return __filtered

def search_data_file():
    try:
        __locator = listdir(main_dir)
        __indexed = sorted([(path.getsize(file), file) for file in __locator])
        return str(__indexed[-1][1])
    except:
        return False

def main_program():
    for __item in zip_file_list:
        if file_check(__item):
            extract_zip_file(__item)
        else:
            download_zip_data_files(url, __item)
            return main_program()

    index_entidades = read_excel(catalog, sheet_name="Catálogo de ENTIDADES", header=0)
    index_municipios = read_excel(catalog, sheet_name="Catálogo MUNICIPIOS", header=0)
    # index = read_excel(read_excel(catalog, sheet_name="Catálogo de ENTIDADES", header=0))
    data_file = search_data_file()
    database = read_csv(data_file, encoding="latin1")
    print("casos Mexico " + str(len(database.axes[0])))
    chiapas = filter_data_by("ENTIDAD_RES","07", database, int, False)
    filter_data_by("FECHA_DEF","9999-99-99", chiapas, str, True)
    tuxtla = filter_data_by("MUNICIPIO_RES","101", chiapas, int, False)
    filter_data_by("FECHA_DEF", "9999-99-99", tuxtla, str, True)
    print(search_on_catalog("07", False, index_entidades))
    print(search_on_catalog("07","101", index_municipios))
main_program()
