# Databricks notebook source
# MAGIC %run "./Reader_Factory"

# COMMAND ----------

class Extractor:
    """
    Abstract class
    """
    def __init__(self):
        pass
    
    def Extract(self):
        pass

class AirpodsAfterIphoneExtrctor(Extractor):
    """
    Implement the steps for extract or reading data
    """
    def Extract(self):
        TransactionInputDF = GetDataSource(
            Data_Type = "csv",
            File_Path = "dbfs:/FileStore/tables/Transaction_Updated.csv"
        ).Get_Data_Frame()

        TransactionInputDF.orderBy("customer_id", "transaction_date").show()

        CustomerInputDF = GetDataSource(
            Data_Type = "delta",
            File_Path = "default.customer_delta_table"
        ).Get_Data_Frame()

        CustomerInputDF.show()

        InputDFs = {
                        "TransactionInputDF" : TransactionInputDF,
                        "CustomerInputDF" : CustomerInputDF    
                   }
        
        return InputDFs 