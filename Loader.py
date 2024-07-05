# Databricks notebook source
# MAGIC %run "./Loader_Factory"

# COMMAND ----------

class Loader:
    def __init__(self, TransformedDF):
        self.TransformedDF = TransformedDF
    
    def Sink(self):
        pass

class AirpodsAfterIphoneLoader(Loader):
    def Sink(self):
        GetSinkSource(
            SinkType = "DBFS",
            DF       = self.TransformedDF,
            Path     = "dbfs:/FileStore/tables/AppleAnalysis/output/AirpodsAfterIphone",
            Method   = "overwrite"
        ).LoadDataFrame()

class OnlyAirpodsAndIphoneLoader(Loader):
    def Sink(self):
        GetSinkSource(
            SinkType = "DeltaTable",
            DF       = self.TransformedDF,
            Path     = "default.Only_AirpodsAndIphone",
            Method   = "overwrite"
        ).LoadDataFrame()

class DelayBetweenBuyingIphoneAndAirpodsLoader(Loader):
    def Sink(self):
        GetSinkSource(
            SinkType = "DBFS",
            DF       = self.TransformedDF,
            Path     = "dbfs:/FileStore/tables/AppleAnalysis/output/Delay_BetweenBuyingIphoneAndAirpods",
            Method   = "overwrite"
        ).LoadDataFrame()