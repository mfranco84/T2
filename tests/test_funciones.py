# from funciones import (
#     transformar_a_parquet,
#     calculate_articles_by_date,
#     calculate_research_group_stats,
#     calculate_research_areas_per_person,
#     calculate_person_most_references,
#     calculate_percentiles
# )
# from pyspark.sql import Row, SparkSession

# spark = SparkSession.builder.appName("UnitTest").getOrCreate()




# def test_transformar_a_parquet(spark):
#     # Mock data representing the raw JSON structure
#     data = [
#         Row(message=Row(
#             DOI="10.1000/1",
#             author=[Row(given="John", family="Doe", ORCID="0000-0001", affiliation=[Row(name="Univ A")], 
#                         **{"authenticated-orcid": True, "name": "John Doe", "sequence": "first", "suffix": ""})],
#             institution=[Row(name="Univ A")],
#             **{"group-title": "Group A", 
#                "created": Row(**{"date-parts": [[2023, 1, 1]], "date-time": "2023-01-01T00:00:00Z"}),
#                "prefix": "10.1000",
#                "reference": [Row(DOI="10.1001/ref1", ISSN="1234", **{"article-title": "Ref 1", "author": "Auth 1", "doi-asserted-by": "publisher", "first-page": "1", "issn-type": "print", "issue": "1", "journal-title": "Journal 1", "key": "k1", "unstructured": "...", "volume": "1", "volume-title": "V1", "year": "2022"})],
#                "link": [Row(URL="http://link.com", **{"content-type": "text/html", "content-version": "1.0", "intended-application": "text-mining"})],
#                "subtype": "journal-article"}
#         ))
#     ]
#     raw_df = spark.createDataFrame(data)
    
#     transformed_df = transformar_a_parquet(raw_df)
    
#     assert "doi" in transformed_df.columns
#     assert "inst" in transformed_df.columns
#     assert transformed_df.count() == 1
#     assert transformed_df.collect()[0]["doi"] == "10.1000/1"

# def test_calculate_articles_by_date(spark):
#     data = [
#         Row(createdDate="2023-01-15T10:00:00Z"),
#         Row(createdDate="2023-01-20T11:00:00Z"),
#         Row(createdDate="2023-02-01T12:00:00Z")
#     ]
#     df = spark.createDataFrame(data)
    
#     result_df = calculate_articles_by_date(df)
#     results = result_df.collect()
    
#     assert len(results) == 2
#     # Row for Jan 2023
#     assert results[0]["created_year"] == 2023
#     assert results[0]["created_month"] == 1
#     assert results[0]["total_articles"] == 2
#     # Row for Feb 2023
#     assert results[1]["created_month"] == 2
#     assert results[1]["total_articles"] == 1

# def test_calculate_research_areas_per_person(spark):
#     data = [
#         Row(doi="1", groupTitle="Area 1", author=[Row(given="John", family="Doe")]),
#         Row(doi="2", groupTitle="Area 2", author=[Row(given="John", family="Doe")]),
#         Row(doi="3", groupTitle="Area 1", author=[Row(given="Jane", family="Smith")]),
#         Row(doi="4", groupTitle="Area 1", author=[Row(given="", family="")]) # Should be unknown
#     ]
#     df = spark.createDataFrame(data)
    
#     result_df = calculate_research_areas_per_person(df)
#     results = result_df.collect()
    
#     # Check John Doe
#     john_doe = [r for r in results if r["full_name"] == "John Doe"][0]
#     assert john_doe["total"] == 2
#     assert "Area 1" in john_doe["research_areas"]
#     assert "Area 2" in john_doe["research_areas"]
    
#     # Check unknown
#     unknown = [r for r in results if r["full_name"] == "unknown"][0]
#     assert unknown["total"] == 1

# def test_calculate_person_most_references(spark):
#     data = [
#         Row(doi="1", author=[Row(given="A", family="1")], 
#             reference=[Row(DOI="ref1"), Row(DOI="ref2"), Row(DOI=None)]),
#         Row(doi="2", author=[Row(given="A", family="1"), Row(given="B", family="2")], 
#             reference=[Row(DOI="ref3")])
#     ]
#     df = spark.createDataFrame(data)
    
#     result_df = calculate_person_most_references(df)
#     results = result_df.collect()
    
#     assert len(results) == 1
#     assert results[0]["full_name"] == "A 1"
#     # A 1 has: (ref1, ref2) from doi 1 + (ref3) from doi 2 = 3 references with DOI
#     assert results[0]["total_references"] == 3

# def test_calculate_percentiles(spark):
#     # Create data to have specific percentiles
#     data = []
#     # 4 people with 10, 20, 30, 40 references with DOI
#     for i, count in enumerate([10, 20, 30, 40]):
#         data.append(Row(doi=str(i), author=[Row(given="P", family=str(i))], 
#                         reference=[Row(DOI="ref")] * count))
    
#     df = spark.createDataFrame(data)
#     quantiles = calculate_percentiles(df)
    
#     assert len(quantiles) == 3
#     assert quantiles[0] >= 10
#     assert quantiles[2] <= 40
