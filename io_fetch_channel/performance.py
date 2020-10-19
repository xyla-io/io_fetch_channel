import pandas as pd

from datetime import datetime, timedelta
from typing import Dict, List
from azrael import SnapchatAPI, SnapchatReporter
from heathcliff import SearchAdsAPI, SearchAdsReporter
from hazel import GoogleAdsAPI, GoogleAdsReporter
from .context import Channel, TimeGranularity, EntityGranularity

class ChannelPerformanceFetcher:
  channel: Channel
  time_granularity: TimeGranularity
  entity_granularity: EntityGranularity
  raw_performance_columns: List[str]

  def __init__(self, raw_channel: str, raw_time_granularity: str, raw_entity_granularity: EntityGranularity, raw_performance_columns: List[str]):
    self.channel = Channel(raw_channel)
    self.time_granularity = TimeGranularity(raw_time_granularity)
    self.entity_granularity = EntityGranularity(raw_entity_granularity)
    self.raw_performance_columns = raw_performance_columns
  
  @property
  def channel_time_granularity(self) -> str:
    if self.channel is Channel.google_ads:
      if self.time_granularity is TimeGranularity.hourly:
        return 'hourly'
      elif  self.time_granularity is TimeGranularity.daily:
        return 'daily'
    elif self.channel is Channel.snapchat:
      if self.time_granularity is TimeGranularity.hourly:
        return 'HOUR'
      elif self.time_granularity is TimeGranularity.daily:
        return 'DAY'
    elif self.channel is Channel.apple_search_ads:
      if self.time_granularity is TimeGranularity.hourly:
        return 'HOURLY'
      elif self.time_granularity is TimeGranularity.daily:
        return 'DAILY'

  @property
  def channel_entity_granularity(self) -> str:
    if self.channel is Channel.google_ads:
      if self.entity_granularity is EntityGranularity.campaign:
        return 'campaign'
      elif self.entity_granularity is EntityGranularity.adgroup:
        return 'ad_group'
      elif self.entity_granularity is EntityGranularity.ad:
        return 'ad'
    elif self.channel is Channel.snapchat:
      if self.entity_granularity is EntityGranularity.campaign:
        return 'campaign'
      elif self.entity_granularity is EntityGranularity.adgroup:
        return 'adsquad'
      elif self.entity_granularity is EntityGranularity.ad:
        return 'ad'
    elif self.channel is Channel.apple_search_ads:
      if self.entity_granularity is EntityGranularity.campaign:
        return 'campaign'
      elif self.entity_granularity is EntityGranularity.adgroup:
        return 'adgroup'
      elif self.entity_granularity is EntityGranularity.ad:
        return 'keyword'
  
  @property
  def channel_performance_columns(self) -> List[str]:
    if self.channel is Channel.google_ads:
      return []
    elif self.channel is Channel.snapchat:
      return [
        'impressions',
        'swipes',
        'spend',
      ]
    elif self.channel is Channel.apple_search_ads:
      return []

  def run(self, credentials: Dict[str, any], start: datetime, end: datetime) -> pd.DataFrame:
    if self.channel is Channel.google_ads:
      api = GoogleAdsAPI(**credentials)
      reporter = GoogleAdsReporter(api=api)
      report = reporter.get_performance_report(
        start_date=start,
        end_date=end,
        entity_granularity=self.channel_entity_granularity,
        time_granularity=self.channel_time_granularity
      )
      report = reporter.add_selected_conversions(
        report=report,
        start_date=start,
        end_date=end,
        entity_granularity=self.channel_entity_granularity,
        time_granularity=self.channel_time_granularity
      )

    elif self.channel is Channel.snapchat:
      api = SnapchatAPI(**credentials)
      api.load_ad_account()
      reporter = SnapchatReporter(api=api)
      now = datetime.utcnow()
      start_date = reporter.clamped_date_in_account_timezone(
        date=start,
        now=now
      )
      end_date = reporter.clamped_date_in_account_timezone(
        date=end + timedelta(days=1),
        now=now
      )
      report = reporter.get_performance_report(
        time_granularity=self.channel_time_granularity,
        entity_granularity=self.channel_entity_granularity,
        columns=self.channel_performance_columns,
        entity_columns=[
          'id',
          'name',
        ],
        start_date=start_date,
        end_date=end_date
      )

    elif self.channel is Channel.apple_search_ads:
      api = SearchAdsAPI(certificates=credentials)
      reporter = SearchAdsReporter(api=api)
      request_overrides = {
        'granularity': self.channel_time_granularity,
      }
      if self.entity_granularity is EntityGranularity.campaign:
        report = reporter.get_campaigns_report(
          start_date=start,
          end_date=end,
          request_overrides=request_overrides
        )
      elif self.entity_granularity is EntityGranularity.adgroup:
        report = reporter.get_adgroups_report(
          start_date=start,
          end_date=end,
          request_overrides=request_overrides
        )
      elif self.entity_granularity is EntityGranularity.ad:
        report = reporter.get_keywords_report(
          start_date=start,
          end_date=end,
          request_overrides=request_overrides
        )
    
    self.process(report=report)
    return report
  
  def process(self, report: pd.DataFrame):
    if self.channel is Channel.google_ads:
      report.rename(lambda s: s.replace('#', '_'), axis='columns', inplace=True)
      report.metrics_cost_micros = report.metrics_cost_micros / 1000000
      report.rename({'metrics_cost_micros': 'cost'}, axis='columns', inplace=True)
      report.drop(columns=['metrics_conversions_value', 'campaign_selective_optimization_conversion_actions'], inplace=True)
      report['installs'] = report.total_conversions - report.selected_conversions
    elif self.channel is Channel.snapchat:
      report.spend = report.spend / 1000000