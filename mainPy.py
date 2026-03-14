import sys
import os
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'Python_scripts'))

import MSP_parsing


start = time.time()

MSP_parsing.parse_MSP('Data/MSP', 'Data/MSP_parsed')

end = time.time()
print(f"Время выполнения:{((end-start)/60):.1f} минут.")