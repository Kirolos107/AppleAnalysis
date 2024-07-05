# Databricks notebook source
from pyspark.sql import Window
from pyspark.sql.functions import lead, col, broadcast, collect_set, array_contains, size, avg, to_timestamp, datediff

class Transformer:
    def __init__(self):
        pass

    def Transform(self, InpputDFs):
        pass

# AirpodsAfterIphoneTransformer Pipeline :
###########################################
class AirpodsAfterIphoneTransformer(Transformer):
    def Transform(self, InputDFs):
        """
        Customers who have bought AirPods after buying the iPhone
        """
        # Get the source file
        TransactionInputDF = InputDFs.get("TransactionInputDF")

        print("TransactionInputDF in Transform")
        TransactionInputDF.show()


        # Add new column to get the next product bought by customer
        WindowSpec = Window.partitionBy("customer_id").orderBy("transaction_date")
        TransformedDF = (TransactionInputDF.
                        withColumn("next_product_name", lead("product_name").over(WindowSpec)))

        print("AirPods after buying iPhone")
        TransformedDF.show()

        # Filter the DF to display only AirPods after buying iPhone 
        FilteredDF = TransformedDF.filter(
            (col("product_name") == "iPhone") & (col("next_product_name") == "AirPods"))

        print("Customers who have bought AirPods after buying the iPhone")
        FilteredDF.orderBy("customer_id", "transaction_date", "product_name").show()

        # Joining Process
        CustomerInputDF = InputDFs.get("CustomerInputDF")
        JoinedDF = CustomerInputDF.join(broadcast(FilteredDF),"customer_id")

        print("Joined DF")
        JoinedDF.show()


        return (JoinedDF.
               select("customer_id",
                      "customer_name",
                      "product_name", 
                      "transaction_date", 
                      "location"))
        

# OnlyAirpodsAndIphoneTransformer Pipeline :
#############################################
class OnlyAirpodsAndIphoneTransformer(Transformer):
    def Transform(self, InputDFs):
        """
        Customers who have bought only AirPods and iPhone, Noting else
        """        
        # Get the source file
        TransactionInputDF = InputDFs.get("TransactionInputDF")
        
        print("TransactionInputDF in Transform")
        TransactionInputDF.show()


        # Collect the bought products by customer in set
        GroupedDF = TransactionInputDF.groupBy("customer_id").agg(collect_set("product_name").alias("products"))
        
        print("GroupedDF")
        GroupedDF.show()


        # Filter the set to only contains Airpods or iPhone
        FilteredDF = GroupedDF.filter(
            (array_contains(col("products"), "AirPods")) &
            (array_contains(col("products"), "iPhone")) &
            (size(col("products")) == 2))
        
        print("Customers who have bought only AirPods and iPhone, Noting else")
        FilteredDF.orderBy("customer_id", "products").show()


        # Joining Process to get customers names
        CustomerInputDF = InputDFs.get("CustomerInputDF")
        JoinedDF = CustomerInputDF.join(broadcast(FilteredDF),"customer_id")

        print("Joined DF")
        JoinedDF.show()


        return (JoinedDF.
               select("customer_id",
                      "customer_name",
                      "products", 
                      "location"))
        

# DelayBetweenBuyingIphoneAndAirpodsTransformer Pipeline :
##################################################################
class DelayBetweenBuyingIphoneAndAirpodsTransformer(Transformer):
    def Transform(self, InputDFs):
        """
        Customers who have bought AirPods after buying the iPhone
        """
        # Get the source file
        TransactionInputDF = InputDFs.get("TransactionInputDF")
        
        print("TransactionInputDF in Transform")
        TransactionInputDF.show()


        # Filter the DF to display only Airpods & iPhone products
        FilteredDF = (TransactionInputDF.filter((col("product_name") == "iPhone")  | (col("product_name") == "AirPods"))
                                   .orderBy("customer_id"))
        print("Filtered DF")
        FilteredDF.show()


        # Add new column to get the next product bought by customer 
        WindowSpec = Window.partitionBy("customer_id").orderBy("transaction_date")
        TransformedDF = (FilteredDF.
                        withColumn("next_product_name", lead("product_name").over(WindowSpec)).
                        withColumn("next_transaction_date", lead("transaction_date").over(WindowSpec)))  
       
        print("Transformed DF")
        TransformedDF.show()


        # Filter the DF to display only (Airpods lead by iPhone) or (iPhone lead by Airpods)
        GroupedDF = (TransformedDF.filter(((col("product_name") == "iPhone")   & (col("next_product_name") == "AirPods")) |
                                           ((col("product_name") == "AirPods") & (col("next_product_name") == "iPhone")))
                                   .orderBy("customer_id"))
       
        print("GroupedDF DF")
        GroupedDF.show()
        

        # Get the time delay
        DelayDF = GroupedDF.withColumn("time_delay", datediff(col("next_transaction_date"), col("transaction_date"))).orderBy("customer_id")
       
        print("Delay DF")
        DelayDF.show()


        # Joining Process to get customers names
        CustomerInputDF = InputDFs.get("CustomerInputDF")
        JoinedDF = CustomerInputDF.join(broadcast(DelayDF),"customer_id")
        print("Joined DF")
        JoinedDF.select("customer_id",  "customer_name", "product_name", "next_product_name", "time_delay").show()


        return (JoinedDF.
               select("customer_id",
                      "customer_name",
                      "product_name", 
                      "next_product_name", 
                      "time_delay"))