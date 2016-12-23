# -*- coding: utf-8 -*-

__author__ = 'Sara Geleskie Damiano'
__contact__ = 'sdamiano@stroudcenter.org'

"""
Created by Sara Geleskie Damiano on 12/2/2016 at 1:53 PM


"""

import numpy as np
import pandas as pd

filepath = "R:\CostaRica/Weather Station/Data/wuhist_all.sas7bdat"

wuhist = pd.read_sas(filepath, format='sas7bdat', index='datetime_utc')
wuhist.dtypes
wuhist.head(5)
# wuhist.index = wuhist.index.tz_localize('UTC').tz_convert('US/Eastern')