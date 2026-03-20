import sys
import os
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'Python_scripts'))

import MSP_parsing
import MSP_aggregator


start = time.time()

""" парсит единый реестр МСП """
# MSP_parsing.parse_MSP('Data/MSP', 'Data/MSP_parsed')

""" агрегирует месячные данные до года """

df = MSP_aggregator.process_year(list(range(2017, 2027)))

""" тесты правильности агрегации """

# df_list = MSP_aggregator.test_agg(2019)

end = time.time()
print(f"Время выполнения:{(end-start):.1f} секунд.")