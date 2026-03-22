# 1. Импорт пакетов
using ZipFile, EzXML, DataFrames, Arrow, Dates

println("🧵 Потоков доступно: $(Threads.nthreads())")

# ─────────────────────────────────────────────────────────────
# Функция парсинга реестра ССЧР
# ─────────────────────────────────────────────────────────────

# Прямой перебор дочерних элементов намного быстрее XPath findall()
    function parse_sschr(content::String)
    doc = parsexml(content)
    
    # Определяем типы колонок с поддержкой missing
    # Тип Int для года и месяца оставляем строгим, так как без даты запись обычно бесполезна
    records = NamedTuple{(:inn, :year, :month, :headcount), 
                         Tuple{Union{String, Missing}, Int, Int, Union{Float64, Missing}}}[]
    
    # Резервируем память (примерно 900 строк на файл)
    sizehint!(records, 950)

    for doc_elem in eachelement(doc.root)
        nodename(doc_elem) != "Документ" && continue

        # Значения по умолчанию (используем Union{T, Missing})
        inn_one::Union{String, Missing} = missing
        workers::Union{Float64, Missing} = missing
        day, month, year = 1, 1, 2000 # Технический дефолт для даты

        # Быстрый проход по вложенным узлам
        for sub_elem in eachelement(doc_elem)
            name = nodename(sub_elem)
            if name == "СведНП"
                inn_one = haskey(sub_elem, "ИННЮЛ") ? sub_elem["ИННЮЛ"] : missing
            elseif name == "СведССЧР"
                if haskey(sub_elem, "КолРаб")
                    # parse(Float64, ...) может выдать ошибку, если в XML пустая строка ""
                    val = sub_elem["КолРаб"]
                    workers = isempty(val) ? missing : parse(Float64, val)
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
# Обработка одного ZIP-файла
# ─────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────
# главная функция
# ─────────────────────────────────────────────────────────────

xml_content = read("D:/sme-support/Data/for_parsing/VO_OTKRDAN_3_9965_9965_20251225_00d6d097-19b1-4670-8221-419acab2b4e2.xml", String)
df = DataFrame(parse_sschr(xml_content))
