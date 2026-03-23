# 1. Импорт пакетов
using ZipFile, EzXML, DataFrames, Parquet2

println("🧵 Потоков доступно: $(Threads.nthreads())")

# ─────────────────────────────────────────────────────────────
# Функция парсинга реестра ССЧР
# ─────────────────────────────────────────────────────────────

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

process_zip(raw"D:\sme-support\Data\for_parsing\data-20251225-structure-20200408.zip", raw"D:\sme-support\Data\out_of_parsing\sschr.parquet", parse_sschr)
