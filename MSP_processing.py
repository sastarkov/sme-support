
import lxml.etree as ET

def parse_msp_xml(file_path):

    tree = ET.parse(file_path)
    root = tree.getroot()

    documents = root.findall(".//Документ")

    for doc in documents:

        if doc.get('ВидСубМСП') != '1':       
            continue                            # пропускаем ИП и глав КФХ, которые не являются юридическими лицами
    
        # пропускаем бессмысленные записи, которые невозможно использовать (нет ИНН, даты состояния) без привлечения дополнительных источников данных
        
        status_date = doc.get('ДатаСост')
        if not status_date:
            continue

        elem_org = doc.find('ОргВклМСП')

        inn = elem_org.get('ИННЮЛ')
        if not inn:
            continue

        """Соберем данные из атрибутов элемента 'Документ'"""
        incl_date = doc.get('ДатаВклМСП')      # дата включения в реестр МСП: ДД.ММ.ГГГГ
        category = doc.get('КатСубМСП')        # категория МСП('КатСубМСП'): 1 – микропредприятие, 2 – малое, 3 – среднее
        sign_new = doc.get('ПризНовМСП')       # признак вновь созданного: 1 – да, 2 – нет
        sign_social = doc.get('СведСоцПред')   # признак социального предприятия: 1 Код региона– да, 2 – нет
        headcount = doc.get('ССЧР')            # среднесписочная численность работников
        """Соберем данные из атрибутов элемента 'Документ'"""

        """Соберем данные из атрибутов дочернего к 'Документ' элемента 'ОргВклМСП'"""
        abbr_name = elem_org.get('НаимОргСокр')
        """Соберем данные из атрибутов дочернего к 'Документ' элемента 'ОргВклМСП'"""

        """Соберем данные из атрибутов дочернего к 'Документ' элемента 'СведМН'"""
        elem_loc = doc.find('СведМН')
        region = elem_loc.get('КодРегион')

        """Соберем данные из атрибутов дочернего к 'Документ' элемента 'СвОКВЭД'"""
        elem_main_OKVED = doc.find('СвОКВЭД/СвОКВЭДОсн')
        main_OKVED = elem_main_OKVED.get('КодОКВЭД')

        elem_add_OKVED = doc.findall('СвОКВЭД/СвОКВЭДДоп')
        add_OKVED = [elem.get('КодОКВЭД') for elem in elem_add_OKVED]

        """Соберем данные из атрибутов дочернего к 'Документ' элемента 'СвЛиценз'"""
        elem_license = doc.findall('СвЛиценз')
        license_number_list = [elem.get('НомЛиценз') for elem in elem_license]
        license = 1 if any(x is not None for x in license_number_list) else 0

        """Соберем данные из атрибутов дочернего к 'Документ' элемента 'СвПрод'"""
        elem_product = doc.findall('СвПрод')
        product_code = [elem.get('КодПрод') for elem in elem_product]
        
        """Соберем данные из атрибутов дочернего к 'Документ' элемента 'СвПрогПарт'"""
        elem_partnership = doc.find('СвПрогПарт')
        partnership = 1 if elem_partnership is not None else 0

        # запись в виде списка

        record = {
            'inn': inn,
            'name': abbr_name, 
            'region': region,
            'year_month': status_date,
            'incl_date': incl_date,
            'category': category,
            'new': sign_new,
            'social': sign_social,
            'headcount': headcount
        }

        print(record)

parse_msp_xml('MSP/2017.xml')
        
        











          


    

