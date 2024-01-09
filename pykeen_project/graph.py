from neo4j import GraphDatabase

uri = "bolt://localhost:7687"  # Replace with your Neo4j connection URI
username = "neo4j"     # Replace with your Neo4j username
password = "zeeshan123"     # Replace with your Neo4j password


# Function to execute the Cypher queries
def execute_queries(driver):
    with driver.session() as session:
        # Define parameters for file paths, constraints, and other details
        parameters = {
            "file_path_root": "D:\task\task2",  # Change to the folder containing the files
            "file_0": "country.csv",
            "file_1": "Population.csv",
            "file_2": 'Monetary.csv',
            "file_3": 'fossil CO2 by country.csv',
            "file_4": 'Total CO2 emission.csv',
            "idsToSkip": []
        }

        # Create constraints
        constraints_query = [
            """
            CREATE CONSTRAINT imp_uniq_Country_countryID IF NOT EXISTS
            FOR (n:Country)
            REQUIRE (n.countryID) IS UNIQUE
            """,
            """
            CREATE CONSTRAINT imp_uniq_Population_countryID IF NOT EXISTS
            FOR (n:Population)
            REQUIRE (n.countryID) IS UNIQUE
            """,
            """
            CREATE CONSTRAINT `imp_uniq_Monetary_countryID` IF NOT EXISTS
            FOR (n: `Monetary`)
            REQUIRE (n.`countryID`) IS UNIQUE;
            """,
            """
            CREATE CONSTRAINT `imp_uniq_Total Emission_Substance` IF NOT EXISTS
            FOR (n: `Total Emission`)
            REQUIRE (n.`Substance`) IS UNIQUE;
            """,
            """
            CREATE CONSTRAINT `imp_uniq_Region_region` IF NOT EXISTS
            FOR (n: `Region`)
            REQUIRE (n.`region`) IS UNIQUE;
            """,
            """
            CREATE CONSTRAINT `imp_uniq_CountryID_country` IF NOT EXISTS
            FOR (n: `CountryID`)
            REQUIRE (n.`country`) IS UNIQUE;
            """,
            """
            CREATE CONSTRAINT `imp_uniq_Emission By Sector_CountryID` IF NOT EXISTS
            FOR (n: `Emission By Sector`)
            REQUIRE (n.`CountryID`) IS UNIQUE;
            """,
            """
            CREATE CONSTRAINT `imp_uniq_Sectors_Sector` IF NOT EXISTS
            FOR (n: `Sectors`)
            REQUIRE (n.`Sector`) IS UNIQUE;
            """,
            """
            CREATE CONSTRAINT `imp_uniq_Substance _Substance` IF NOT EXISTS
            FOR (n: `Substance `)
            REQUIRE (n.`Substance`) IS UNIQUE;
            """
        ]

        # Execute constraint creation queries
        for constraint_query in constraints_query:
            session.run(constraint_query)

        # Define the NODE load queries
        node_load_queries = [
            """
            // NODE load 1 - Country
            LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
            WITH row
            WHERE NOT row.countryID IN $idsToSkip AND NOT row.countryID IS NULL
            CALL {
              WITH row
              MERGE (n:Country { countryID: row.countryID })
              SET n.countryID = row.countryID,
                  n.country = row.country,
                  n.region = row.region
            } IN TRANSACTIONS OF 10000 ROWS
            """,
            """
            // NODE load 2 - Population
            LOAD CSV WITH HEADERS FROM ($file_path_root + $file_1) AS row
            WITH row
            WHERE NOT row.countryID IN $idsToSkip AND NOT row.countryID IS NULL
            CALL {
              WITH row
              MERGE (n:Population { countryID: row.countryID })
              SET n.countryID = row.countryID,
                  n.`2013` = toInteger(trim(row.`2013`)),
                  // ... (other SET operations for different years)
                  n.`2023` = toInteger(trim(row.`2023`))
            } IN TRANSACTIONS OF 10000 ROWS
            """,
            """
            // NODE load 3 - Monetary
            LOAD CSV WITH HEADERS FROM ($file_path_root + $file_2) AS row
            WITH row
            WHERE NOT row.`countryID` IN $idsToSkip AND NOT row.`countryID` IS NULL
            CALL {
            WITH row
            MERGE (n: `Monetary` { `countryID`: row.`countryID` })
            SET n.`countryID` = row.`countryID`
            SET n.`monetary` = row.`monetary`
            SET n.`yr2013` = row.`yr2013`
            SET n.`yr2014` = row.`yr2014`
            SET n.`yr2015` = row.`yr2015`
            SET n.`yr2016` = row.`yr2016`
            SET n.`yr2017` = row.`yr2017`
            SET n.`yr2018` = row.`yr2018`
            SET n.`yr2019` = row.`yr2019`
            SET n.`yr2020` = row.`yr2020`
            SET n.`yr2021` = row.`yr2021`
            SET n.`yr2022` = row.`yr2022`
            SET n.`yr2023` = row.`yr2023`
            } IN TRANSACTIONS OF 10000 ROWS;
            """,
            """
            
            LOAD CSV WITH HEADERS FROM ($file_path_root + $file_4) AS row
            WITH row
            WHERE NOT row.`Substance` IN $idsToSkip AND NOT row.`Substance` IS NULL
            CALL {
            WITH row
            MERGE (n: `Total Emission` { `Substance`: row.`Substance` })
            SET n.`Substance` = row.`Substance`
            SET n.`Total CO2 produced` = row.`Total CO2 produced`
            SET n.`yr2013` = toFloat(trim(row.`yr2013`))
            SET n.`yr2014` = toFloat(trim(row.`yr2014`))
            SET n.`yr2015` = toFloat(trim(row.`yr2015`))
            SET n.`yr2016` = toFloat(trim(row.`yr2016`))
            SET n.`yr2017` = toFloat(trim(row.`yr2017`))
            SET n.`yr2018` = toFloat(trim(row.`yr2018`))
            SET n.`yr2019` = toFloat(trim(row.`yr2019`))
            SET n.`yr2020` = toFloat(trim(row.`yr2020`))
            SET n.`yr2021` = toFloat(trim(row.`yr2021`))
            } IN TRANSACTIONS OF 10000 ROWS;
            """,
            """
            
            LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
            WITH row
            WHERE NOT row.`region` IN $idsToSkip AND NOT row.`region` IS NULL
            CALL {
            WITH row
            MERGE (n: `Region` { `region`: row.`region` })
            SET n.`region` = row.`region`
            } IN TRANSACTIONS OF 10000 ROWS;
            """,
            """
            
            LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
            WITH row
            WHERE NOT row.`countryID` IN $idsToSkip AND NOT row.`countryID` IS NULL
            CALL {
            WITH row
            MERGE (n: `CountryID` { `country`: row.`countryID` })
            SET n.`country` = row.`countryID`
            SET n.`countryID` = row.`countryID`
            } IN TRANSACTIONS OF 10000 ROWS;
            """,
            """
            
            LOAD CSV WITH HEADERS FROM ($file_path_root + $file_3) AS row
            WITH row
            WHERE NOT row.`CountryID` IN $idsToSkip AND NOT row.`CountryID` IS NULL
            CALL {
            WITH row
            MERGE (n: `Emission By Sector` { `CountryID`: row.`CountryID` })
            SET n.`CountryID` = row.`CountryID`
            SET n.`Substance` = row.`Substance`
            SET n.`Sector` = row.`Sector`
            SET n.`yr2013` = toFloat(trim(row.`yr2013`))
            SET n.`yr2014` = toFloat(trim(row.`yr2014`))
            SET n.`yr2015` = toFloat(trim(row.`yr2015`))
            SET n.`yr2016` = toFloat(trim(row.`yr2016`))
            SET n.`yr2017` = toFloat(trim(row.`yr2017`))
            SET n.`yr2018` = toFloat(trim(row.`yr2018`))
            SET n.`yr2019` = toFloat(trim(row.`yr2019`))
            SET n.`yr2020` = toFloat(trim(row.`yr2020`))
            SET n.`yr2021` = toFloat(trim(row.`yr2021`))
            } IN TRANSACTIONS OF 10000 ROWS;
            """,
            
            """
            
            LOAD CSV WITH HEADERS FROM ($file_path_root + $file_3) AS row
            WITH row
            WHERE NOT row.`Sector` IN $idsToSkip AND NOT row.`Sector` IS NULL
            CALL {
            WITH row
            MERGE (n: `Sectors` { `Sector`: row.`Sector` })
            SET n.`Sector` = row.`Sector`
            } IN TRANSACTIONS OF 10000 ROWS;
            """,
            """
            
            LOAD CSV WITH HEADERS FROM ($file_path_root + $file_3) AS row
            WITH row
            WHERE NOT row.`Substance` IN $idsToSkip AND NOT row.`Substance` IS NULL
            CALL {
            WITH row
            MERGE (n: `Substance ` { `Substance`: row.`Substance` })
            SET n.`Substance` = row.`Substance`
            } IN TRANSACTIONS OF 10000 ROWS;
            """
        ]

        # Execute the queries
        for query in node_load_queries:
            session.run(query, parameters)

# Establish the connection to Neo4j
driver = GraphDatabase.driver(uri, auth=(username, password))

# Execute the queries (constraints creation and NODE load)
execute_queries(driver)

# Close the connection
driver.close()