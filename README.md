# T20WorldCupAnalysis
Overview

• Developed a comprehensive solution to create an 11-player team for the T20World Cup, ensuring an average score of at least 180 runs and the ability to defend against an  average of 150 runs. Implemented real-time web scraping from ESPN Cricinfo using Python Selenium to collect player statistics and match data.

• Used Python Pandas for data cleaning and transformation, ensuring accuracy and consistency in the dataset. Utilized Power Query for further data transformation and enhancement. Implemented data modeling and parameterization using DAX (Data Analysis Expressions) to derive meaningful insights and metrics from the dataset.

• Designed and built a comprehensive dashboard using Power BI to visualize key performance indicators and metrics. Analyzed insights collected from the dashboard to make informed decisions in selecting the final 11 players for the T20World Cup team, considering both batting and bowling capabilities.



Web Scraping

    Prerequisites
        - Python 3.x
        - Selenium library
        - Re module
        - CSV library
        - Chrome Web Driver

    It utilizes Selenium to interact with the ESPNcricinfo website and extract match and player links. It then processes these links to obtain detailed match and player statistics.

    It generates the following CSV files in the dataset directory:
        - match_summary.csv: Contains match details and batting statistics.
        - match_batting_summary.csv: Contains detailed batting statistics.
        - match_bowling_summary.csv: Contains detailed bowling statistics.
        - players_links.csv: Contains player links for further processing.
    
    Player Data Processing
    It also processes player data by visiting each player link and extracting relevant information. The resulting players.csv file contains the following columns:
        - name: Player's name
        - team: Player's team
        - image: Player's image URL
        - battingStyle: Player's batting style
        - bowlingStyle: Player's bowling style
        - playingRole: Player's playing role
        - description: Player's description
    
    Limitations
    The code is designed to work with the specific URL provided in the code.
    The code may not be able to handle changes in the ESPNcricinfo website structure.


Pre-Processing

    Prerequisites
        - Python 3.x
        - Pandas library
    
    The c0de utilizes Pandas to read and manipulate CSV files generated from the web scraping code.
    
    The code expects the following CSV files in the dataset directory:
        - match_summary.csv: Contains match details and batting statistics.
        - match_batting_summary.csv: Contains detailed batting statistics.
        - match_bowling_summary.csv: Contains detailed bowling statistics.
        - players.csv: Contains player links for further processing.


Power BI    
    Team Selection Criteria
        Overview
            - The team should be able to score at least 180 runs on an average
            - They should be to defend 150 runs on an average
        Criteria
            
            Opener (2 Player, 1 Left Hand, 1 Right Hand)
                Batting Average - (> 30)
                Strike Rate - (> 140)
                Innings Batted - (> 3)
                Boundary % - (> 50)
                Batting Position - (< 4)
            
            Middle Order (3 Player)
                Batting Average - (> 40)
                Strike Rate - (> 125)
                Innings Batted - (> 3)
                Avg. Balls Faced - (> 20)
                Batting Position - (> 2)

            Finisher / Lower Order (2 Player)
                Batting Average - (> 25)
                Strike Rate - (> 130)
                Innings Batted > 3
                Avg. Balls - (> 1)2
                Batting Position - (> 4)
                Innings Bowled - (> 1)
            
            ALL-ROUNDERS / LOWER ORDER
                Batting Average Average runs scored in an innings - (> 15)
                Strike Rate No of runs scored per 100 balls - (> 140)
                Innings Batted - (> 2)
                Batting Position - (> 4)
                Innings Bowled - (> 2)
                Bowling Economy - (< 7)
                Bowling Strike Rate - (< 20)

            


i am creating a combined end-to-end project of data analytics & data engineering  

Problem Statement :- you need to create a team from t20 world cup such that that team securing a minimum 180-run average and defending against 150 runs.
Web scrape real time data from espn about t20 world cup. performed etl on the scrape data and created a dynamic power bi dashboard.
tools used: 
Python Selenium on EC2. 
Stored staged scraped dataset on s3.
Created first crawler in aws glue to convert json data to database format in glue catalogue
created first etl job to extract data from glue catalogue table and convert to parquet format and applied some transform of datatypes and performed data cleaning
created second crawler to fetch table schema from redshift
created second etl job to load data into redshift
created a connection between power bi and redshift

def MyTransform (glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import col, when
    # Convert the input DynamicFrame to a DataFrame
    df = dfc.select(list(dfc.keys())[0]).toDF()
    
    # Create a temporary view for the DataFrame
    df.createOrReplaceTempView("inputTable")
    
    # Apply the transformations using Spark SQL
    df = df.withColumn("sr", when(col("sr") == "null", 0).otherwise(col("sr")))
    df = df.withColumn("out", when(col("out") == "out", 1).otherwise(0))
    df = df.withColumn("out", df["out"].cast("integer"))
    df = df.withColumn("Boundary Runs", (4 * col("4s")) + (6 * col("6s")))
    # Convert the transformed DataFrame to a DynamicFrame
    dyf_transformed = DynamicFrame.fromDF(df, glueContext, "result0")
    
    # Return the transformed DynamicFrameCollection
    return DynamicFrameCollection({"CustomTransform0": dyf_transformed}, glueContext)

    df = dfc.select(list(dfc.keys())[0]).toDF()
    df.createOrReplaceTempView("inputTable")
    df = df.withColumn("over_parts", split(df["overs"], "\\."))
    
    df = df.withColumn("overs1", col("over_parts")[0] / col("over_parts")[1])
    df = df.withColumn("overs2", when(col("over_parts")[1] != 0, col("over_parts")[1] ).otherwise(0))

    df = df.withColumn("overs2", when(col("overs2") == "null", 0).otherwise(col("overs2")))

    df = df.drop("over_parts")
    df = df.drop("overs)

    df = df.withColumn("Balls", (6 * col("overs1")) + (col("overs2")))

    dyf_transformed = DynamicFrame.fromDF(df, glueContext, "result0")
    
    # Return the transformed DynamicFrameCollection
    return DynamicFrameCollection({"CustomTransform0": dyf_transformed}, glueContext)



