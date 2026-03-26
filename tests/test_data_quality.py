import pytest
from pyspark.sql import SparkSession

# Create Spark session (fixture)
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[*]") \
        .appName("pytest-example") \
        .getOrCreate()

# Test 1: runtime should be positive
def test_runtime_positive(spark):
    df = spark.table("silver_episodes")
    assert df.filter("runtime <= 0").count() == 0

# Test 2: Required fields must not be null
def test_no_null_ids(spark):
    df = spark.table("silver_shows")
    assert df.filter("id IS NULL").count() == 0

def test_genre_not_empty(spark):
    df = spark.table("silver_shows")
    assert df.filter("genres IS NULL").count() == 0

# Test 3: Show names must be unique per ID
def test_unique_show_ids(spark):
    df = spark.table("silver_shows")
    assert df.count() == df.select("id").distinct().count()