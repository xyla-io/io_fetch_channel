from enum import Enum

class TimeGranularity(Enum):
  hourly = 'hourly'
  daily = 'daily'

class Channel(Enum):
  apple_search_ads = 'apple_search_ads'
  google_ads = 'google_ads'
  snapchat = 'snapchat'

class EntityGranularity(Enum):
  campaign = 'campaign'
  adgroup = 'adgroup'
  ad = 'ad'