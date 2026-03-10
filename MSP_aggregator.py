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

        # Регион – первая мода,чтобы исключить ошибочные записи.
        pl.col("region").mode().first().alias("region"),
      
        # преимущественная категория 
        pl.col("category").mode().first().alias("prefer_category"),

        pl.col("sign_new").mode().first().alias("sign_new"),

        pl.col("sign_social").mode().first().alias("sign_social"),

        # Средняя численность за первое полугодие (мес. 1-6)
        pl.col("headcount").filter(pl.col("month").is_between(1, 6)).mean().cast(pl.Float64).alias("headcount_1h"),

        # Средняя численность за второе полугодие (мес. 7-12)
        pl.col("headcount").filter(pl.col("month").is_between(7, 12)).mean().cast(pl.Float64).alias("headcount_2h"),

        # Последнее известное значение основного ОКВЭД (благодаря сортировке берём last())
        pl.col("main_OKVED").mode().first().alias("main_okved"),

        (pl.col("license") == "1").any().alias("has_license")
    ])


if __name__ == "__main__":

# 1. Открываем датасет (лениво)
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
        "headcount": pl.Int64,
        "main_OKVED": pl.String,
        "other_OKVED": pl.List,
        "license": pl.String
        }
)  # предполагается, что данные сохранены в этой папке

# 2. Применяем фильтры (год и категория)
    lf = lf.filter(
        pl.col("year") == 2020
           # здесь "3" как строка, в ваших данных может быть число
    )

    start = time.time()

# 3. Агрегируем
    result = aggregate_msp(lf).collect()

    end = time.time()

    print(f"Время выполнения:{((end-start)/60):.1f} минут.")