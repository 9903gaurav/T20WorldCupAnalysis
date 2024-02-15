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

