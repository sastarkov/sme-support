# 1. Импорт пакетов
using ZipFile, EzXML, DataFrames, Arrow, Dates

println("🧵 Потоков доступно: $(Threads.nthreads())")

# ─────────────────────────────────────────────────────────────
# Функция парсинга реестра ССЧР
# ─────────────────────────────────────────────────────────────
function parse_sschr(content::String)
    doc = readxml(IOBuffer(content))
    records = Vector{NamedTuple{(:inn, :year, :month, :headcount), Tuple{String, Int, Int, Float64}}}(undef, 0)
    
    for doc_elem in findall("./Документ", doc.root)
        sved = findfirst("./СведНП", doc_elem)
        sschr = findfirst("./СведССЧР", doc_elem)
        
        inn_one = isnothing(sved) ? "" : attribute(sved, "ИННЮЛ")
        workers = isnothing(sschr) ? "" : parse(Float64, attribute(sschr, "КолРаб"))

        date_sost = attribute(doc_elem, "ДатаСост")
        day_one, month_one, year_one = parse.(Int, split(date_sost, "."))

        # year_one = parse(Int, split(date_sost, ".")[3])
        # month_one = parse(Int, split(date_sost, ".")[2])

        push!(records, (inn=inn_one, year=year_one, month=month_one, headcount=workers))
    end
    
    return records
end

# ─────────────────────────────────────────────────────────────
# Обработка одного ZIP-файла
# ─────────────────────────────────────────────────────────────
function process_zip_file(zip_path::String, parser_func::Function)
    r = ZipFile.Reader(zip_path)
    xml_files = [f for f in r.files if endswith(f.name, ".xml") && !endswith(f.name, "/")]
    
    results = Vector{Vector{NamedTuple}}(undef, length(xml_files))
    
    Threads.@threads for i in eachindex(xml_files)
        file = xml_files[i]
        content = read(file, String)
        results[i] = parser_func(content)
    end
    
    close(r)
    return vcat(results...)
end

# ─────────────────────────────────────────────────────────────
# 6. Главная функция
# ─────────────────────────────────────────────────────────────
function main(input_dir::String, output_dir::String)
    println("🚀 Старт ETL-пайплайна...")
    println("🧵 Потоков: $(Threads.nthreads())")
    println("📂 Входная папка: $input_dir")
    println("📂 Выходная папка: $output_dir")
    println("─" ^ 60)
    
    t_start = time()
    
    # Шаг 1: Обработка всех ZIP
    records = process_zip_file(input_dir)
    
    # Шаг 2: Создание DataFrame
    df = DataFrame(records)
    
    println("─" ^ 60)
    println("📈 Статистика:")
    println("   Уникальных лет: $(length(unique(df.Год)))")
    println("   Уникальных месяцев: $(length(unique(df.Месяц)))")
    println("   Всего записей: $(nrow(df))")
    println("─" ^ 60)
    
    # Шаг 3: Сохранение с партиционированием
    save_partitioned_parquet(df, output_dir)
    
    t_elapsed = time() - t_start
    
    println("─" ^ 60)
    println("⏱ Время выполнения: $(round(t_elapsed, digits=2)) сек")
    println("🎉 Готово!")
end



