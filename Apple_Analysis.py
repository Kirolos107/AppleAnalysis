# Databricks notebook source
# MAGIC %run "./Extractor"

# COMMAND ----------

# MAGIC %run "./Transformer"

# COMMAND ----------

# MAGIC %run "./Loader"

# COMMAND ----------

class FirstWorkFlow:
    """
    ETL pipeline to generate the data for all customers who have bought Airpods after buying Iphone 
    """
    def __init__(self):
        pass

    def Runner(self):
        # Step 1: Extract the data from different sources
        InputDFs = AirpodsAfterIphoneExtrctor().Extract()

        # Step 2: Implement the Transformation logic 
        # Customers who have bought AirPods after buying the iPhone
        AirpodsAfterIphoneTransformer_DF = AirpodsAfterIphoneTransformer().Transform(InputDFs)

        # Step 3: Load all required data to different sink
        AirpodsAfterIphoneLoader(AirpodsAfterIphoneTransformer_DF).Sink()

# COMMAND ----------

class SecondWorkFlow:
    """
    ETL pipeline to generate the data for all customers who have bought only Airpods and Iphone, Noting else 
    """
    def __init__(self):
        pass

    def Runner(self):
        # Step 1: Extract the data from different sources
        InputDFs = AirpodsAfterIphoneExtrctor().Extract()

        # Step 2: Implement the Transformation logic 
        # Customers who have bought only AirPods and iPhone, Noting else
        OnlyAirpodsAndIphoneTransformer_DF = OnlyAirpodsAndIphoneTransformer().Transform(InputDFs)

        # Step 3: Load all required data to different sink
        OnlyAirpodsAndIphoneLoader(OnlyAirpodsAndIphoneTransformer_DF).Sink()

# COMMAND ----------

class ThirdWorkFlow:
    """
    ETL pipeline to generate the average time delay buying Iphone and buying Airpods for each customer 
    """
    def __init__(self):
        pass

    def Runner(self):
        # Step 1: Extract the data from different sources
        InputDFs = AirpodsAfterIphoneExtrctor().Extract()

        # Step 2: Implement the Transformation logic 
        # Average time delay buying Iphone and buying Airpods for each customer
        DelayBetweenBuyingIphoneAndAirpodsTransformer_DF = DelayBetweenBuyingIphoneAndAirpodsTransformer().Transform(InputDFs)

        # Step 3: Load all required data to different sink
        DelayBetweenBuyingIphoneAndAirpodsLoader(DelayBetweenBuyingIphoneAndAirpodsTransformer_DF).Sink()

# COMMAND ----------

class WorkFlowRunner:
    def __init__(self, name):
        self.name = name

    def Runner(self):
        if self.name == "FirstWorkFlow":
            FirstWorkFlow().Runner()
        elif self.name == "SecondWorkFlow":
            SecondWorkFlow().Runner()
        elif self.name == "ThirdWorkFlow":
            ThirdWorkFlow().Runner()
        else:
            ValueError(f"Not implemented for: {self.name}")

Name = "ThirdWorkFlow"
WorkFlow_Runner = WorkFlowRunner(Name).Runner()            