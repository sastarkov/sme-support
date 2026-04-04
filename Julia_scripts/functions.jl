# 1. Импорт пакетов
using ZipFile, EzXML, DataFrames, Parquet2

println("🧵 Потоков доступно: $(Threads.nthreads())")

# ─────────────────────────────────────────────────────────────
# Тип записи для парсера мер поддержки (support measures)
# ─────────────────────────────────────────────────────────────
const Record = @NamedTuple{
    inn::String, # ИНН фирмы СвЮл\ИННЮЛ
    year_state::Int, # год из даты состояния записи
    month_state::Int,  # месяц из даты состояния записи 
    date_state::Date,  # дата состояния записи Документ\ДатаСост ?
    date_first::Union{Date, Missing}, #Дата первичной публикации поддержки в реестре СвПредПод\ДатаСвед
    date_last::Union{Date, Missing}, #Дата последнего обновления в реестре сведений о поддержке СвПредПод\ДатаОбнов
    inn_prov::Union{String, Missing}, #ИНН органа, предоставившего поддержку СвПредПод\ИННЮЛ
    cat::Union{String, Missing}, #= Категория субъекта малого и среднего предпринимательства 
    на дату принятия решения о предоставлении поддержки 1 – микропредприятие | 2 – малое предприятие 
    | 3 – среднее предприятие | 4 – отсутствует СвПредПод\КатСуб =#
    date_supp::Union{Date, Missing}, # Срок оказания поддержки СвПредПод\СрокПод
    date_desion_support::Union{Date, Missing}, # Дата принятия решения о предоставлении поддержки СвПредПод\ДатаПрин
    date_desion_terminate::Union{Date, Missing}, # Дата принятия решения о прекращении оказания поддержки; Дата в формате ДД.ММ.ГГГГ СвПредПод\ДатаПрекр; случай проблем!
    indicator_offense::Union{Bool, Missing}, # Факт наличия нарушения 1 - да | 2 - нет СвПредПод\ИнфНаруш
    support_form::Union{String, Missing}, # Код формы предоставленной поддержки, СвПредПод\ФормПод\КодФорм
    support_name_form::Union{String, Missing}, # Наименование формы предоставленной поддержки, СвПредПод\ФормПод\НаимФорм
    support_type::Union{String, Missing}, # Код вида предоставленной поддержки, СвПредПод\ВидПод\КодВид
    support_name_type::Union{String, Missing}, # Наименование вида предоставленной поддержки СвПредПод\ВидПод\НаимВид
    support_amount::Union{Int, Missing}, # Размер поддержки СвПредПод\РазмПод\РазмПод
    support_unit::Union{String, Missing}, #= Единица измерения поддержки. Принимает значение:
    1 – рубль | 2 – квадратный метр | 3 – час | 4 – процент | 5 – единица СвПредПод\РазмПод\ЕдПод =#
    offense_type::Union{Bool, Missing}, #= Вид нарушения. Принимает значение: 1 – Нарушение порядка и условий оказания поддержки, 
    связанное с нецелевым использованием средств поддержки или представлением недостоверных сведений и документов | 2 – Нарушение порядка и условий 
    оказания поддержки, не связанное с нецелевым использованием средств поддержки или представлением недостоверных сведений и документов 
    СвПредПод\Нарушения\ВидНаруш =#
}

# ─────────────────────────────────────────────────────────────
# Безопасное получение атрибута
# ─────────────────────────────────────────────────────────────
function safe_attr(node, attr_name::String, default=Missing)
    if isnothing(node)
        return default
    end
    return hasattribute(node, attr_name) ? attribute(node, attr_name) : default
end

# ─────────────────────────────────────────────────────────────
# Безопасное получение даты
# ─────────────────────────────────────────────────────────────
function safe_parse_date(date_str::Union{String, Missing, Nothing})
    
    (isnothing(date_str) || ismissing(date_str) || isempty(date_str)) && return missing
    dt = tryparse(Date, date_str, dateformat"dd.MM.yyyy")

    return isnothing(dt) ? missing : dt
end

# ─────────────────────────────────────────────────────────────
# Функция парсинга реестра ССЧР
# ─────────────────────────────────────────────────────────────
function parse_xml_msppp(xml_bytes)
    
    doc = readxml(IOBuffer(String(xml_bytes)))
    root = doc.root
    
    records = Record[]
    sizehint!(records, 1000)  # Предварительное выделение памяти для словаря
    
    for doc_node in children(root)

        nodename(doc_node) == "Документ" || continue
        
        # ИНН
        inn_node = findfirst("./СвЮЛ", doc_node)
        inn = !isnothing(inn_node) ? safe_attr(inn_node, "ИННЮЛ") : missing
        ismissing(inn) && continue  # Пропускаем записи без ИНН

        #Дата состояния
        date_state = safe_parse_date(safe_attr(doc_node, "ДатаСост"))
        
        for supp in findall("./СвПредПод", doc_node)
            #Дата первичной публикации
            date_first = safe_parse_date(safe_attr(supp, "ДатаСвед"))
            #Дата последнего обновления
            date_last = safe_parse_date(safe_attr(supp, "ДатаОбнов"))
            #ИНН органа, предоставившего поддержку
            inn_prov = safe_attr(supp, "ИННЮЛ")
            # Категория субъекта малого и среднего предпринимательства 
            cat = safe_attr(supp, "КатСуб")
            # Срок оказания поддержки
            date_supp = safe_parse_date(safe_attr(supp, "СрокПод"))
            # Дата принятия решения о предоставлении поддержки
            date_desion_support = safe_parse_date(safe_attr(supp, "ДатаПрин"))
            # Дата принятия решения о прекращении оказания поддержки
            date_desion_terminate = safe_parse_date(safe_attr(supp, "ДатаПрекр"))
            # Факт наличия нарушения
            indicator_offense = safe_attr(supp, "ИнфНаруш")

            form = findfirst("./ФормПод", supp)
            # Код формы предоставленной поддержки
            support_form = safe_attr(form, "КодФорм")
            # Наименование формы предоставленной поддержки
            # !!!!!!!!!!!

            # # Форма поддержки
            # form = findfirst("./ФормПод", supp)
            # if form !== nothing
            #     form_code = safe_attr(form, "КодФорм")
            #     form_name = safe_attr(form, "НаимФорм")
            # else
            #     form_code = form_name = ""
            # end

            # # Вид поддержки
            # kind = findfirst("./ВидПод", supp)
            # if kind !== nothing
            #     kind_code = safe_attr(kind, "КодВид")
            #     kind_name = safe_attr(kind, "НаимВид")
            # else
            #     kind_code = kind_name = ""
            # end

            # # Размер поддержки
            # amount_elem = findfirst("./РазмПод", supp)
            # if amount_elem !== nothing
            #     amount = safe_attr(amount_elem, "РазмПод")
            #     unit = safe_attr(amount_elem, "ЕдПод")
            # else
            #     amount = unit = ""
            # end

            # Добавляем запись

            push!(records, Record(
                inn = inn,
                # Проверяем дату на missing перед вызовом функций года/месяца
                year_state  = ismissing(date_state) ? missing : year(date_state),
                month_state = ismissing(date_state) ? missing : month(date_state),
                date_state  = date_state,
                date_first = date_first,
                date_last = date_last,  # !!!!!!!!!!!!!!!!!
                date_decision = date_decision_parsed,
                date_start    = date_start_parsed,
                category      = category,
                form_code     = form_code,
                form_name     = form_name,
                kind_code     = kind_code,
                kind_name     = kind_name,
                amount        = amount,
                unit          = unit,
                info_violation = info_violation,
            ))
        end
    end
    
    return records
end



function parse_sschr(content::String)

    doc = parsexml(content)
    
    # Определяем типы колонок с поддержкой missing
    # Тип Int для года и месяца оставляем строгим, так как без даты запись обычно бесполезна
    records = NamedTuple{(:inn, :year, :month, :headcount), 
                        Tuple{Union{String, Missing}, Int, Int, Union{Float64, Missing}}}[]
    

    for doc_elem in eachelement(doc.root)
            nodename(doc_elem) != "Документ" && continue

        # Значения по умолчанию
        inn_one::Union{String, Missing} = missing
        workers::Union{Float64, Missing} = missing
        day, month, year = 5, 5, 5555  # Технический дефолт для даты

        # Быстрый проход по вложенным узлам
        for sub_elem in eachelement(doc_elem)
            name = nodename(sub_elem)
            if name == "СведНП"
                val_inn = strip(haskey(sub_elem, "ИННЮЛ") ? sub_elem["ИННЮЛ"] : "")
                inn_one = isempty(val_inn) ? missing : val_inn
            elseif name == "СведССЧР"
                if haskey(sub_elem, "КолРаб")
                val_kr = strip(sub_elem["КолРаб"])
                # Превращаем пустые строки и пробелы в missing
                workers = isempty(val_kr) ? missing : tryparse(Float64, val_kr)
                end
            end
        end

    # Парсинг даты из атрибута Документа
    ds = haskey(doc_elem, "ДатаСост") ? doc_elem["ДатаСост"] : ""
    if length(ds) >= 10
        day   = parse(Int, ds[1:2])
        month = parse(Int, ds[4:5])
        year  = parse(Int, ds[7:10])
    end

    push!(records, (inn=inn_one, year=year, month=month, headcount=workers))
    end
    return records
end

# ─────────────────────────────────────────────────────────────
# Функция парсинга реестра МСП - получателей поддержки
# ─────────────────────────────────────────────────────────────

# function parse_xml_msppp(content::String)






# end


# ─────────────────────────────────────────────────────────────
# Обработка одного ZIP-файла
# ─────────────────────────────────────────────────────────────

function process_zip(zip_path, output_path, parse_function::Function)
    z = ZipFile.Reader(zip_path)
    
    # Отбираем только XML файлы
    xml_entries = filter(f -> endswith(f.name, ".xml"), z.files)
    num_files = length(xml_entries)
    
    println("Шаг 1: Чтение $num_files файлов в оперативную память...")
    # Читаем содержимое последовательно, чтобы не сломать ZipFile
    file_contents = Vector{String}(undef, num_files)
    for i in 1:num_files
        try
            file_contents[i] = read(xml_entries[i], String)
        catch e
            close(z)
            error("ОШИБКА ЧТЕНИЯ АРХИВА в файле '$(xml_entries[i].name)': $e")
        end
    end
    close(z) # Архив больше не нужен, он в памяти

    println("Шаг 2: Парсинг на $(Threads.nthreads()) потоках...")
    all_results = Vector{Any}(undef, num_files)

    # Вот теперь включаем параллелизм на полную мощность
    Threads.@threads for i in 1:num_files
        try
            all_results[i] = parse_function(file_contents[i])
        catch e
            error("КРИТИЧЕСКАЯ ОШИБКА ПАРСИНГА в файле №$i: $e")
        end
    end

    println("Шаг 3: Сборка и сохранение...")
    # Собираем все кусочки в один большой массив и превращаем в DataFrame
    final_df = DataFrame(reduce(vcat, all_results))
    
    # Удаляем временный массив строк, чтобы освободить память перед записью
    file_contents = nothing 
    GC.gc() 

    Parquet2.writefile(output_path, final_df)
end

# ─────────────────────────────────────────────────────────────
# главная функция
# ─────────────────────────────────────────────────────────────

# xml_content = read("D:/sme-support/Data/for_parsing/VO_OTKRDAN_3_9965_9965_20251225_00d6d097-19b1-4670-8221-419acab2b4e2.xml", String)
# df = DataFrame(parse_sschr(xml_content))

# process_zip(raw"D:\sme-support\Data\for_parsing\data-20251225-structure-20200408.zip", raw"D:\sme-support\Data\out_of_parsing\sschr.parquet", parse_sschr)
