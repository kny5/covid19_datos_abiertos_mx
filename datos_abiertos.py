import requests
from zipfile import ZipFile
from pandas import read_csv


url = "http://187.191.75.115/gobmx/salud/datos_abiertos/datos_abiertos_covid19.zip"
file_name = "datos_covid19_mx.zip"

# print("Iniciando Descarga")
# datos_zip = requests.get(url, allow_redirects=True)
#
# print("Descarga finalizada")
# open(file_name, 'wb').write(datos_zip.content)
# print("Guardada")
# # Create a ZipFile Object and load sample.zip in it
# print("Extrayendo ZIP")
# with ZipFile(file_name, 'r') as zipObj:
#     # Extract all the contents of zip file in current directory
#     zipObj.extractall()
# csv_name = zipObj.namelist()[0]
csv_name = "200517COVID19MEXICO.CSV"
print("CSV extraido")
print("Leyendo CSV")
dataframe = read_csv(csv_name, encoding="latin1")
print("Leído CSV")
# print(dataframe.head())
# print(dataframe.tail())
bool_datos_chiapas_residentes = dataframe['ENTIDAD_RES'] == int("07")
datos_chiapas = dataframe[bool_datos_chiapas_residentes]
datos_chiapas_tuxtla = datos_chiapas['MUNICIPIO_RES'] == int("101")
filtro_tuxtla = datos_chiapas[datos_chiapas_tuxtla]
filtro_tuxtla.to_csv('out.csv', encoding='UTF-8')
print("End")
