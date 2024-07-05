# Databricks notebook source
class DataSink:
    """
    Abstract class
    """
    def __init__(self, DF, Path, Method, Params):
        self.DF     = DF
        self.Path   = Path
        self.Method = Method
        self.Params = Params

    def LoadDataFrame(self):
        """
        Abstract method, Function will be defined in sub-classes
        """
        raise ValueError("Not Implemented")

class LoadToDBFS(DataSink):
    def LoadDataFrame(self):
        self.DF.write.mode(self.Method).save(self.Path)

class LoadToDBFS_WithPartition(DataSink):
    def LoadDataFrame(self):
        PartitionByColumns = self.Params.get("PartitionByColumns")
        self.DF.write.mode(self.Method).partitionBy(*PartitionByColumns).save(self.Path)

class LoadToDeltaTable(DataSink):
    def LoadDataFrame(self):
        self.DF.write.format("delta").mode(self.Method).saveAsTable(self.Path)

def GetSinkSource(SinkType, DF, Path, Method, Params = None):
    if SinkType == "DBFS":
        return LoadToDBFS(DF, Path, Method, Params)
    elif SinkType == "DBFS_WithPartition":
        return LoadToDBFS_WithPartition(DF, Path, Method, Params)
    elif SinkType == "DeltaTable":
        return LoadToDeltaTable(DF, Path, Method, Params)
    else:
        return ValueError(f"Not implemented for sink type: {SinkType}")

