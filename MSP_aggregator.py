import polars as pl
import time

def aggregate_msp(lf):
    """
    Агрегирует помесячные данные реестра МСП до уровня фирма-год.
    Ожидается, что входной LazyFrame содержит колонки:
    inn, year, month, date, inclusion_date, region, category,
    sign_new, sign_social, headcount, main_okved, other_okved, license.
    """
        
    return lf.group_by("inn", "year").agg([
        # Первый и последний месяц присутствия фирмы в реестре в текущем году
        pl.col("month").min().alias("first_month"),
        pl.col("month").max().alias("last_month"),

        # Самая ранняя дата включения в реестр
        pl.col("inclusion_date").min().alias("previous_start_date"),

        # Регион – мода (самое частое значение). Если несколько мод, берём первую.
        pl.col("region").mode().first().alias("region"),

        # Количество месяцев с категорией 1 и 2 (используем фильтрацию)
        # pl.col("category").filter(pl.col("category") == "1").count().alias("cat_1_count"),
        # pl.col("category").filter(pl.col("category") == "2").count().alias("cat_2_count"),

        # преимущественная категория 
        pl.col("category").mode().first().alias("prefer_category"),

        # # Тренд категории: сравнение последнего и первого значения
        # (pl.when(pl.col("category").last() > pl.col("category").first())
        #  .then(1)
        #  .when(pl.col("category").last() < pl.col("category").first())
        #  .then(-1)
        #  .otherwise(0).alias("category_trend")),

        # # Количество месяцев с признаком "новое предприятие" (sign_new == "1")
        # pl.col("sign_new").filter(pl.col("sign_new") == "1").count().alias("count_new"),

        pl.col("sign_new").mode().first().alias("sign_new"),

        # Количество месяцев с признаком "социальное предприятие" (sign_social == "1")
        # pl.col("sign_social").filter(pl.col("sign_social") == "1").count().cast(pl.Int64).alias("count_social"),

        pl.col("sign_social").mode().first().alias("sign_social"),

        # Средняя численность за первое полугодие (мес. 1-6)
        # pl.col("headcount").filter(pl.col("month").is_between(1, 6)).mean().cast(pl.Float64).alias("headcount_1h"),

        # Средняя численность за второе полугодие (мес. 7-12)
        # pl.col("headcount").filter(pl.col("month").is_between(7, 12)).mean().cast(pl.Float64).alias("headcount_2h"),

        # Последнее известное значение основного ОКВЭД (благодаря сортировке берём last())
        pl.col("main_OKVED").mode().first().alias("main_okved"),

        # Последний известный список дополнительных ОКВЭД
        # pl.col("other_OKVED").mode().first().alias("other_okved"),

        # Количество месяцев, когда у фирмы была лицензия (license == "1")
        # pl.col("license").filter(pl.col("license") == "1").count().alias("count_license"),

        (pl.col("license") == "1").any().alias("has_license")
    ])


if __name__ == "__main__":

# 1. Открываем датасет (лениво)
lf = pl.scan_parquet("MSP_parsed/"
# schema={
#         "inn": pl.String,
#         "year": pl.Int32,
#         "month": pl.Int32,
#         "date": pl.Date,
#         "inclusion_date": pl.Date,
#         "region": pl.String,
#         "category": pl.String,
#         "sign_new": pl.String,
#         "sign_social": pl.String,
#         "headcount": pl.Float64,
#         "main_OKVED": pl.String,
#         "other_OKVED": pl.List,
#         "license": pl.String,
#     }
)  # предполагается, что данные сохранены в этой папке

# 2. Применяем фильтры (год и категория)
lf = lf.filter(
    pl.col("year") == 2017,
    pl.col("category") == "3"   # здесь "3" как строка, в ваших данных может быть число
)

start = time.time()

# 3. Агрегируем
result = aggregate_msp(lf).collect()

end = time.time()

print(f"Время выполнения:{((end-start)/60):.1f} минут.")