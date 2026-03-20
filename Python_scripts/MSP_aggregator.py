import polars as pl
import pyarrow.dataset as ds

def prepare_datasetMSP(dataset_path):

    lf = pl.scan_parquet(dataset_path)

    keep_columns = [
    "inn", "month", "inclusion_date",
    "category", "sign_new", "sign_social", 
    "headcount", "license"
    ]

    target_types = {
    "inn": pl.String,
    "month": pl.Int32,
    "inclusion_date": pl.Date,
    "category": pl.String,
    "sign_new": pl.String,
    "sign_social": pl.String,
    "headcount": pl.Float64,
    "license": pl.String,
    }

    lf_clean = lf.select(keep_columns)

    cast_exprs = [pl.col(col).cast(dtype).alias(col) for col, dtype in target_types.items()]
    lf_clean = lf_clean.with_columns(cast_exprs)

    lf_clean.sink_parquet(

        pl.PartitionBy("MSP_parsed2/",
        key = ["month"])  # сохраняем партиционирование

        )

def aggregate_year_msp(lf):
 
    """
    Агрегирует месячные данные реестра МСП до уровня фирма-год.
    """
    # lf = lf.with_columns(pl.col("headcount").cast(pl.Float64))
        
    return lf.group_by("inn", "year").agg([

        pl.len().alias('months'),

        # # Первый и последний месяц присутствия фирмы в реестре в текущем году
        # pl.col("month").min().alias("first_month"),
        # pl.col("month").max().alias("last_month"),

        # Предыдущая дата включения в реестр
        pl.col("inclusion_date").min().alias("previous_start_date"),

        # # Регион – первая мода,чтобы исключить ошибочные записи
        # pl.col("region").mode().first().alias("region"),
      
        # преимущественная категория в течение года: 1 – микропредприятие, 2 – малое, 3 – среднее
        pl.col("category").mode().first().alias("prefer_category"),

        # тренд изменения категории в течение года (разница категорий в старший месяц и в младший в группе):
        # 0 - изменений нет, >0 - переход в след. категорию, <0 - переход в предыдущую
    #    (pl.col("category")
    #        .get(pl.col("month").arg_max())
    #        .cast(pl.Int32) -
    #      pl.col("category")
    #        .get(pl.col("month").arg_min())
    #        .cast(pl.Int32)
    #     ).alias("category_trend"),

        # преобладающее значение фактора признак нового предприятия в течение года:
        # 1 – новое предприятие, 2 – нет
        pl.col("sign_new").filter(pl.col("sign_new") == "1").count().alias("months_new"),
        # pl.col("sign_new").mode().first().alias("sign_new"),

        # преобладающее значение фактора признак социального предприятия в течение года:
        # 1 – социальное предприятие, 2 – нет
        pl.col("sign_social").filter(pl.col("sign_social") == "1").count().alias("months_social"),
        # pl.col("sign_social").mode().first().alias("sign_social"),

        # Средняя численность за первое полугодие (мес. 1-6)
        pl.col("headcount").filter(pl.col("month").is_between(1, 6)).mode().first().alias("headcount_1h"),

        # Средняя численность за второе полугодие (мес. 7-12)
        pl.col("headcount").filter(pl.col("month").is_between(7, 12)).mode().first().alias("headcount_2h"),

        # # ОКВЭД – первая мода,чтобы исключить ошибочные записи
        # pl.col("main_OKVED").mode().first().alias("main_okved"),

        # владела ли фирма хоть одной лицензией в течение года
        (pl.col("license") == "1").any().alias("has_license")
    ])
 
def process_year(list_year):

    for year in list_year:

        print(f"Обработка года: {year}...")
        input_path = "Data/MSP_parsed" 

        lf_year = (
            pl.scan_parquet(f"{input_path}/**/*.parquet", hive_partitioning=True)
            .filter(pl.col("year") == year)
        )
        df_result = aggregate_year_msp(lf_year).collect()
        table = df_result.to_arrow()

        ds.write_dataset(
                table,
                base_dir = "Data/MSP_aggregated", 
                format = 'parquet',
                partitioning = ['year'],
                partitioning_flavor = 'hive',
                existing_data_behavior = 'overwrite_or_ignore',
                )
    return df_result

def test_agg(year):

    df = pl.scan_parquet("Data/MSP_aggregated/").filter(pl.col("year") == year).collect()
    random_inn = df.sample(n=3).get_column("inn").to_list()

    df_annual = df.filter(pl.col("inn").is_in(random_inn))

    df_month = (pl.scan_parquet("Data/MSP_parsed/")
    .filter(pl.col("year") == year)
    .filter(pl.col("inn").is_in(random_inn))
    .collect().sort('inn'))

    print(df_annual)
    print(df_month)
    return df_annual, df_month