import pytest
import os,sys,inspect
current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir) 
from main import sample_transform


@pytest.mark.usefixtures("spark_session")
def test_sample_transform(spark_session):
  test_df = spark_session.createDataFrame(
    [
      ('hobbit', 'Samwise', 5),
      ('hobbit', 'Bilbo', 50),
      ('hobbit', 'Bilbo', 20),
      ('wizard', 'Gandalf', 1000)
    ],
    ['that_column', 'another_column', 'yet_another']
  )

  print("<<<<<<<<< test_df >>>>>>>>>>")
  test_df.show()

  new_df = sample_transform(test_df)
  assert new_df.count() == 1
  assert new_df.toPandas().to_dict('list')['new_column'][0] == 70

  print("<<<<<<<<< new_df >>>>>>>>>>")
  new_df.show()

#  <<<<<<<<< test_df >>>>>>>>>>
#  +-----------+--------------+-----------+
#  |that_column|another_column|yet_another|
#  +-----------+--------------+-----------+
#  |     hobbit|       Samwise|          5|
#  |     hobbit|         Bilbo|         50|
#  |     hobbit|         Bilbo|         20|
#  |     wizard|       Gandalf|       1000|
#  +-----------+--------------+-----------+
#  
#  <<<<<<<<< new_df >>>>>>>>>>
#  +--------------+----------+---------+
#  |another_column|new_column|indicator|
#  +--------------+----------+---------+
#  |         Bilbo|        70|      yes|
#  +--------------+----------+---------+
