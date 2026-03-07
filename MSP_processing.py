
import lxml.etree as ET
import polars as pl

from datetime import datetime

def parse_msp_xml(xml_path):

    records = []

    tree = ET.parse(xml_path)
    root = tree.getroot()

    documents = root.findall(".//Документ")

    for doc in documents:

        """Пропускаем ИП и глав КФХ, которые не являются юридическими лицами"""
        if doc.get('ВидСубМСП') != '1':       
            continue                            
    
        """Пропускаем бессмысленные записи, которые невозможно использовать (нет ИНН, даты состояния) б
        ез привлечения дополнительных источников данных"""
        status_date = doc.get('ДатаСост')
        if not status_date:
            continue

        elem_org = doc.find('ОргВклМСП')
        inn = elem_org.get('ИННЮЛ')
        if not inn:
            continue

        """Извлекаем месяц и год из даты состояния записи""" 
        status_date = datetime.strptime(status_date, "%d.%m.%Y")
        status_month = status_date.strftime("%m")
        status_year = status_date.strftime("%Y")

        """Собираем данные из атрибутов элемента 'Документ'"""
        incl_date = doc.get('ДатаВклМСП')      # дата включения в реестр МСП: ДД.ММ.ГГГГ
        category = doc.get('КатСубМСП')        # категория МСП('КатСубМСП'): 1 – микропредприятие, 2 – малое, 3 – среднее
        sign_new = doc.get('ПризНовМСП')       # признак вновь созданного: 1 – да, 2 – нет
        sign_social = doc.get('СведСоцПред')   # признак социального предприятия: 1 – да, 2 – нет
        headcount = doc.get('ССЧР')            # среднесписочная численность работников
       
        """Собираем данные из атрибутов дочернего к 'Документ' элемента 'ОргВклМСП'"""
        # abbr_name = elem_org.get('НаимОргСокр')

        """Собираем данные из атрибутов дочернего к 'Документ' элемента 'СведМН'"""
        elem_loc = doc.find('СведМН')
        region = elem_loc.get('КодРегион')

        """Собираем данные из атрибутов дочернего к 'Документ' элемента 'СвОКВЭД'"""
        elem_main_OKVED = doc.find('СвОКВЭД/СвОКВЭДОсн')
        main_OKVED = elem_main_OKVED.get('КодОКВЭД')

        elem_add_OKVED = doc.findall('СвОКВЭД/СвОКВЭДДоп')
        other_OKVED = [elem.get('КодОКВЭД') for elem in elem_add_OKVED]

        """Собираем данные из атрибутов дочернего к 'Документ' элемента 'СвЛиценз'"""
        elem_license = doc.findall('СвЛиценз')
        license_number_list = [elem.get('НомЛиценз') for elem in elem_license]
        license = '1' if any(x is not None for x in license_number_list) else '2'  # наличие хотя бы одной лицензии на дату внесения сведений, 1 - есть, 2 - нет

        """Собираем данные из атрибутов дочернего к 'Документ' элемента 'СвПрод'"""
        # elem_product = doc.findall('СвПрод')
        # product_code = [elem.get('КодПрод') for elem in elem_product]
        
        """Собираем данные из атрибутов дочернего к 'Документ' элемента 'СвПрогПарт'"""
        # elem_partnership = doc.find('СвПрогПарт')
        # partnership = 1 if elem_partnership is not None else 0

        """Запись в виде списка"""
        record = {
            'inn': inn,
            'status_year': status_year,
            'status_month': status_month,
            'inclusion_date': incl_date,
            'region': region,
            'category': category,
            'sign_new': sign_new,
            'sign_social': sign_social,
            'headcount': headcount,
            'main_OKVED': main_OKVED,
            'other_OKVED': other_OKVED,
            'license': license
        }

        records.append(record)

    return(records)      

a = parse_msp_xml('MSP/2025.xml')
df = pl.DataFrame(a)

def colect_msp_month(zip_path):{



}
        











          


    

