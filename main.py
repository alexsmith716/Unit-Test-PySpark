import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def sample_transform(input_df: DataFrame) -> DataFrame:
  inter_df = input_df.where(input_df['that_column'] == \
    F.lit('hobbit')).groupBy('another_column').agg(F.sum('yet_another').alias('new_column'))

  print("<<<<<<<<< inter_df >>>>>>>>>>")
  inter_df.show()

  output_df = inter_df.select('another_column', 'new_column', \
    F.when(F.col('new_column') > 10, 'yes').otherwise('no').alias('indicator')).where(
    F.col('indicator') == F.lit('yes'))
  return output_df

#  <<<<<<<<< inter_df >>>>>>>>>>
#  +--------------+----------+
#  |another_column|new_column|
#  +--------------+----------+
#  |       Samwise|         5|
#  |         Bilbo|        70|
#  +--------------+----------+
