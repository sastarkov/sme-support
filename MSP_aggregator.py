import polars as pl
import time

def aggregate_msp(lf):

    """
    Агрегирует помесячные данные реестра МСП до уровня фирма-год.
    """
    # lf = lf.with_columns(pl.col("headcount").cast(pl.Float64))
        
    return lf.group_by("inn", "year").agg([

        # Первый и последний месяц присутствия фирмы в реестре в текущем году
        pl.col("month").min().alias("first_month"),
        pl.col("month").max().alias("last_month"),

        # Предыдущая дата включения в реестр
        pl.col("inclusion_date").min().alias("previous_start_date"),

        # Регион – первая мода,чтобы исключить ошибочные записи
        pl.col("region").mode().first().alias("region"),
      
        # преимущественная категория в течение года: 1 – микропредприятие, 2 – малое, 3 – среднее
        pl.col("category").mode().first().alias("prefer_category"),

        # тренд изменения категории в течение года (разница категорий в старший месяц и в младший в группе):
        # 0 - изменений нет, >0 - переход в след. категорию, <0 - переход в предыдущую
       (pl.col("category")
           .get(pl.col("month").arg_max())
           .cast(pl.Int32) -
         pl.col("category")
           .get(pl.col("month").arg_min())
           .cast(pl.Int32)
        ).alias("category_trend"),

        # преобладающее значение фактора признак нового предприятия в течение года:
        # 1 – новое предприятие, 2 – нет
        pl.col("sign_new").mode().first().alias("sign_new"),

        # преобладающее значение фактора признак социального предприятия в течение года:
        # 1 – социальное предприятие, 2 – нет
        pl.col("sign_social").mode().first().alias("sign_social"),

        # Средняя численность за первое полугодие (мес. 1-6)
        pl.col("headcount").filter(pl.col("month").is_between(1, 6)).mean().cast(pl.Float64).alias("headcount_1h"),

        # Средняя численность за второе полугодие (мес. 7-12)
        pl.col("headcount").filter(pl.col("month").is_between(7, 12)).mean().cast(pl.Float64).alias("headcount_2h"),

        # ОКВЭД – первая мода,чтобы исключить ошибочные записи
        pl.col("main_OKVED").mode().first().alias("main_okved"),

        # владела ли фирма хоть одной лицензией в течение года
        (pl.col("license") == "1").any().alias("has_license")
    ])


if __name__ == "__main__":

    start = time.time()

    lf = pl.scan_parquet("MSP_parsed/",
    schema={
        "inn": pl.String,
        "year": pl.Int32,
        "month": pl.Int32,
        "inclusion_date": pl.Date,
        "region": pl.String,
        "category": pl.String,
        "sign_new": pl.String,
        "sign_social": pl.String,
        "headcount": pl.Float64,
        "main_OKVED": pl.String,
        "other_OKVED": pl.List,
        "license": pl.String
        }
).filter(pl.col("year") == 2020)

    df = aggregate_msp(lf).collect()

    end = time.time()
    print(f"Время выполнения:{((end-start)/60):.1f} минут.")

    random_inn = df.sample(n=1)['inn'].item()

    df_test = pl.scan_parquet("MSP_parsed/").filter(pl.col("year") == 2020, pl.col("inn") == random_inn).collect()