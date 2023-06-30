from databricks.connect import DatabricksSession
from databricks.sdk.core import Config

config = Config(
    profile = 'DEFAULT',
  cluster_id = '0601-182128-dcbte59m'
)

spark = DatabricksSession.builder.sdkConfig(config).getOrCreate()

# Test creating dummy data
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]

# Create a DataFrame from the data
df = spark.createDataFrame(data, ["name", "age"])
df.show()
