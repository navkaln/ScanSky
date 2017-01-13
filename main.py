import ScanSky
import datetime

print(datetime.datetime.now())


ScanSky.get_cache('BOS')
ScanSky.get_cache('LGA',destinationplace='US')


#ScanSky.get_cache('SFO')

#ScanSky.get_caches(popularity1=175)
ScanSky.read_dump(filters=True)

print(datetime.datetime.now())