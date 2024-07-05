# Databricks notebook source
class DataSource:
    """
    Abstract class
    """

    def __init__(self, path):
        self.path = path

    def Get_Data_Frame(self):
        """
        Abstract mehtod, Function will be defined in sub-classes
        """

        raise ValueError("Not Implemented")

class CSV_DataSource(DataSource):
    def Get_Data_Frame(self):
        return(
                spark.
                read.
                format("csv").
                option("header", "True").
                load(self.path)
              )

class Parquet_DataSource(DataSource):
    def Get_Data_Frame(self):
        return(
                spark.
                read.
                format("parquet").
                load(self.path)
              )

class Delta_DataSource(DataSource):
    def Get_Data_Frame(self):

        Table_Name = self.path
        return(
                spark.
                read.
                table(Table_Name)
              )

def GetDataSource(Data_Type, File_Path):
    if Data_Type == "csv":
        return CSV_DataSource(File_Path)
    if Data_Type == "parquet":
        return Parquet_DataSource(File_Path)
    if Data_Type == "delta":
        return Delta_DataSource(File_Path)
    else:
        raise ValueError(f"Not implemented for data type: {Data_Type}")