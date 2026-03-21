# 1. Импорт пакетов
using ZipFile, EzXML, DataFrames, Arrow, Dates

println("🧵 Потоков доступно: $(Threads.nthreads())")

# 3. Функция парсинга реестра ССЧР

function parse_xml_content(content::String)
    doc = readxml(IOBuffer(content))
    records = Vector{NamedTuple{(:inn, :year, :headcount), Tuple{String, Int, Float64}}}(undef, 0)
    
    for doc_elem in findall("./Документ", doc.root)
        sved = findfirst("./СведНП", doc_elem)
        sschr = findfirst("./СведССЧР", doc_elem)
        
        inn_one = isnothing(sved) ? "" : attribute(sved, "ИННЮЛ")
        date_sost = attribute(doc_elem, "ДатаСост")
        year_one = parse(Int, split(date_sost, ".")[3])
        workers = isnothing(sschr) ? "" : parse(Float64, attribute(sschr, "КолРаб"))
        
        push!(records, (inn=inn_one, year=year_one, headcount=workers))
    end
    
    return records
end



