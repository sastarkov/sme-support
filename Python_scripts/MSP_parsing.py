import lxml.etree as ET
import polars as pl
import zipfile
import logging
import pyarrow.dataset as ds

from datetime import datetime
from pathlib import Path
from tqdm import tqdm

#Функция парсинга одного xml, содержащего записи реестра субъектов малого и среднего предпринимательства с сайта ФНС
def parse_msp_xml(xml_path):

    records = []

    tree = ET.parse(xml_path)
    root = tree.getroot()

    documents = root.findall(".//Документ")

    for doc in documents:
        
        if doc.get('ВидСубМСП') != '1':       
            continue  #пропускаем ИП и глав КФХ, которые не являются юридическими лицами                              
    
        status_date = doc.get('ДатаСост')  #Пропускаем бессмысленные записи, которые невозможно использовать  
        if not status_date:                #(нет ИНН, даты состояния) без привлечения дополнительных источников данных
            continue

        elem_org = doc.find('ОргВклМСП')
        inn = elem_org.get('ИННЮЛ') if elem_org is not None else None
        if not inn:
            continue

        #Извлекаем месяц и год из даты состояния записи
        status_date = datetime.strptime(status_date, "%d.%m.%Y")
        status_month = int(status_date.strftime("%m")) # для совместимости с .parquet д.б. числовой тип
        status_year = int(status_date.strftime("%Y"))  # для совместимости с .parquet д.б. числовой тип

        #Собираем данные из атрибутов элемента 'Документ'
        incl_date_raw = doc.get('ДатаВклМСП')      
        incl_date = datetime.strptime(incl_date_raw, "%d.%m.%Y").date() if incl_date_raw else None  # дата включения в реестр МСП: YYYY.MM.DD

        category = doc.get('КатСубМСП')  # категория МСП('КатСубМСП'): 1 – микропредприятие, 2 – малое, 3 – среднее
        sign_new = doc.get('ПризНовМСП')  # признак вновь созданного: 1 – да, 2 – нет
        sign_social = doc.get('СведСоцПред')  # признак социального предприятия: 1 – да, 2 – нет

        # среднесписочная численность работников
        headcount_raw = doc.get('ССЧР')  
        if headcount_raw is not None: 
            try:
                headcount = float(headcount_raw)
            except ValueError:
                headcount = None  # обработчик ошибки, если преобразование типа не удалось
        else:
            headcount = None
       
        #Собираем данные из атрибутов дочернего к 'Документ' элемента 'СвЛиценз'
        elem_license = doc.findall('СвЛиценз')
        license_number_list = [elem.get('НомЛиценз') for elem in elem_license]
        # наличие хотя бы одной лицензии на дату внесения сведений, 1 - есть, 2 - нет
        license = '1' if any(x is not None for x in license_number_list) else '2'  

        #Формируем итоговую запись по субъекту в словарь 
        record = {
            'inn': inn,
            'year': status_year,
            'month': status_month,
            'inclusion_date': incl_date,
            'category': category,
            'sign_new': sign_new,
            'sign_social': sign_social,
            'headcount': headcount,
            'license': license
        }

        records.append(record) # возвращаем список словарей c данными из одного xml

    return records      

#Функция для сбора в датафрейм месячных данных из zip - файла реестра МСП
def colect_msp_month(zip_path):

    month_data = []

    with zipfile.ZipFile(zip_path, 'r') as z:

        list_xmls = z.namelist() # список имен xml в zip архиве

        for file_xml in tqdm(list_xmls, desc=f"Обработано .xml в {zip_path.name}", leave=True, unit = ' files'):
            try:
                with z.open(file_xml) as f:
                    month_data.extend(parse_msp_xml(f)) # формируем месячный список словарей (из всех xml в zip)
            except Exception as e:
                print(f"\nОшибка при открытии {file_xml} в архиве {zip_path.name}: {e}")
                logging.error(f"В архиве {zip_path.name}, файл {file_xml}: {e}")
    
    schema_overrides = {  # для дальнейшего упорядочивания работы с типами переопределим автоматические
    'inn': pl.String,
    'year': pl.Int32,
    'month': pl.Int32,
    'inclusion_date': pl.Date,
    'category': pl.String,
    'sign_new': pl.String,
    'sign_social': pl.String,
    'headcount': pl.Float64,
    'license': pl.String
}
    return pl.DataFrame(month_data, schema_overrides=schema_overrides)  # возвращаем месячный датафрейм с данными

#Функция парсинга всех исходных *.xml.zip  в указанной папке
def parse_MSP(MSP_in, MSP_out):

    logging.basicConfig(filename='parsing_MSP_errors.log', level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s')

    MSP_path = Path(MSP_in)
    zips = list(MSP_path.glob('*.zip')) # список путей к файлам

    for zip in zips:

        zip_name = zip.stem
        postfix = '-'.join(zip_name.split('-')[:2])

        df = colect_msp_month(zip)
        if len(df) >0:
            table = df.to_arrow() 
            ds.write_dataset(
                table,
                base_dir = MSP_out, 
                format = 'parquet',
                partitioning = ['year', 'month'],
                partitioning_flavor = 'hive',
                existing_data_behavior = 'overwrite_or_ignore',
                basename_template=f"part-{{i}}_from_{postfix}.parquet"  # добавляем постфикс к имени файла с указанием на источник данных
                )







# if __name__ == "__main__":

#     logging.basicConfig(filename='parsing_MSP_errors.log', level=logging.ERROR,
#                     format='%(asctime)s - %(levelname)s - %(message)s')

#     start = time.time()

#     MSP_path = Path('MSP')
#     zips = list(MSP_path.glob('*.zip')) # список путей к файлам

#     for zip in zips:

#         zip_name = zip.stem
#         postfix = '-'.join(zip_name.split('-')[:2])

#         df = colect_msp_month(zip)
#         if len(df) >0:
#             table = df.to_arrow() 
#             ds.write_dataset(
#                 table,
#                 base_dir = 'MSP_parsed', 
#                 format = 'parquet',
#                 partitioning = ['year', 'month'],
#                 partitioning_flavor = 'hive',
#                 existing_data_behavior = 'overwrite_or_ignore',
#                 basename_template=f"part-{{i}}_from_{postfix}.parquet"  # добавляем постфикс к имени файла с указанием на источник данных
#                 )

#     end = time.time()

#     print(f"Время выполнения:{((end-start)/60):.1f} минут.")