:param {
  // Define the file path root and the individual file names required for loading.
  // https://neo4j.com/docs/operations-manual/current/configuration/file-locations/
  file_path_root: 'file:///', // Change this to the folder your script can access the files at.
  file_0: 'country.csv',
  file_1: 'Population.csv',
  file_2: 'Monetary.csv',
  file_3: 'fossil CO2 by country.csv',
  file_4: 'Total CO2 emission.csv'
};

// CONSTRAINT creation
// -------------------
//
// Create node uniqueness constraints, ensuring no duplicates for the given node label and ID property exist in the database. This also ensures no duplicates are introduced in future.
//
//
// NOTE: The following constraint creation syntax is generated based on the current connected database version 5.15-aura.
CREATE CONSTRAINT `imp_uniq_Country_countryID` IF NOT EXISTS
FOR (n: `Country`)
REQUIRE (n.`countryID`) IS UNIQUE;
CREATE CONSTRAINT `imp_uniq_Monetary                                                _countryID` IF NOT EXISTS
FOR (n: `Monetary                                                `)
REQUIRE (n.`countryID`) IS UNIQUE;
CREATE CONSTRAINT `imp_uniq_Population_countryID` IF NOT EXISTS
FOR (n: `Population`)
REQUIRE (n.`countryID`) IS UNIQUE;
CREATE CONSTRAINT `imp_uniq_Total Emission_Substance` IF NOT EXISTS
FOR (n: `Total Emission`)
REQUIRE (n.`Substance`) IS UNIQUE;
CREATE CONSTRAINT `imp_uniq_Region_region` IF NOT EXISTS
FOR (n: `Region`)
REQUIRE (n.`region`) IS UNIQUE;
CREATE CONSTRAINT `imp_uniq_CountryID_country` IF NOT EXISTS
FOR (n: `CountryID`)
REQUIRE (n.`country`) IS UNIQUE;
CREATE CONSTRAINT `imp_uniq_Emission By Sector_CountryID` IF NOT EXISTS
FOR (n: `Emission By Sector`)
REQUIRE (n.`CountryID`) IS UNIQUE;
CREATE CONSTRAINT `imp_uniq_Sectors_Sector` IF NOT EXISTS
FOR (n: `Sectors`)
REQUIRE (n.`Sector`) IS UNIQUE;
CREATE CONSTRAINT `imp_uniq_Substance _Substance` IF NOT EXISTS
FOR (n: `Substance `)
REQUIRE (n.`Substance`) IS UNIQUE;

:param {
  idsToSkip: []
};

// NODE load
// ---------
//
// Load nodes in batches, one node label at a time. Nodes will be created using a MERGE statement to ensure a node with the same label and ID property remains unique. Pre-existing nodes found by a MERGE statement will have their other properties set to the latest values encountered in a load file.
//
// NOTE: Any nodes with IDs in the 'idsToSkip' list parameter will not be loaded.
LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`countryID` IN $idsToSkip AND NOT row.`countryID` IS NULL
CALL {
  WITH row
  MERGE (n: `Country` { `countryID`: row.`countryID` })
  SET n.`countryID` = row.`countryID`
  SET n.`country` = row.`country`
  SET n.`region` = row.`region`
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_2) AS row
WITH row
WHERE NOT row.`countryID` IN $idsToSkip AND NOT row.`countryID` IS NULL
CALL {
  WITH row
  MERGE (n: `Monetary                                                ` { `countryID`: row.`countryID` })
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

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_1) AS row
WITH row
WHERE NOT row.`countryID` IN $idsToSkip AND NOT row.`countryID` IS NULL
CALL {
  WITH row
  MERGE (n: `Population` { `countryID`: row.`countryID` })
  SET n.`countryID` = row.`countryID`
  SET n.`2013` = toInteger(trim(row.`2013`))
  SET n.`2014` = toInteger(trim(row.`2014`))
  SET n.`2015` = toInteger(trim(row.`2015`))
  SET n.`2016` = toInteger(trim(row.`2016`))
  SET n.`2017` = toInteger(trim(row.`2017`))
  SET n.`2018` = toInteger(trim(row.`2018`))
  SET n.`2019` = toInteger(trim(row.`2019`))
  SET n.`2020` = toInteger(trim(row.`2020`))
  SET n.`2021` = toInteger(trim(row.`2021`))
  SET n.`2022` = toInteger(trim(row.`2022`))
  SET n.`2023` = toInteger(trim(row.`2023`))
} IN TRANSACTIONS OF 10000 ROWS;

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

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`region` IN $idsToSkip AND NOT row.`region` IS NULL
CALL {
  WITH row
  MERGE (n: `Region` { `region`: row.`region` })
  SET n.`region` = row.`region`
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`countryID` IN $idsToSkip AND NOT row.`countryID` IS NULL
CALL {
  WITH row
  MERGE (n: `CountryID` { `country`: row.`countryID` })
  SET n.`country` = row.`countryID`
  SET n.`countryID` = row.`countryID`
} IN TRANSACTIONS OF 10000 ROWS;

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

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_3) AS row
WITH row
WHERE NOT row.`Sector` IN $idsToSkip AND NOT row.`Sector` IS NULL
CALL {
  WITH row
  MERGE (n: `Sectors` { `Sector`: row.`Sector` })
  SET n.`Sector` = row.`Sector`
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_3) AS row
WITH row
WHERE NOT row.`Substance` IN $idsToSkip AND NOT row.`Substance` IS NULL
CALL {
  WITH row
  MERGE (n: `Substance ` { `Substance`: row.`Substance` })
  SET n.`Substance` = row.`Substance`
} IN TRANSACTIONS OF 10000 ROWS;


// RELATIONSHIP load
// -----------------
//
// Load relationships in batches, one relationship type at a time. Relationships are created using a MERGE statement, meaning only one relationship of a given type will ever be created between a pair of nodes.
LOAD CSV WITH HEADERS FROM ($file_path_root + $file_2) AS row
WITH row 
CALL {
  WITH row
  MATCH (source: `Country` { `countryID`: row.`countryID` })
  MATCH (target: `Monetary                                                ` { `countryID`: row.`countryID` })
  MERGE (source)-[r: `Economy`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_1) AS row
WITH row 
CALL {
  WITH row
  MATCH (source: `Country` { `countryID`: row.`countryID` })
  MATCH (target: `Population` { `countryID`: row.`countryID` })
  MERGE (source)-[r: `Has population of`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row 
CALL {
  WITH row
  MATCH (source: `Country` { `countryID`: row.`countryID` })
  MATCH (target: `Region` { `region`: row.`region` })
  MERGE (source)-[r: `Location`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row 
CALL {
  WITH row
  MATCH (source: `Country` { `countryID`: row.`countryID` })
  MATCH (target: `CountryID` { `country`: row.`countryID` })
  MERGE (source)-[r: `CountryID`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_3) AS row
WITH row 
CALL {
  WITH row
  MATCH (source: `Emission By Sector` { `CountryID`: row.`CountryID` })
  MATCH (target: `Country` { `countryID`: row.`CountryID` })
  MERGE (source)-[r: `Total Emission by sector`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_3) AS row
WITH row 
CALL {
  WITH row
  MATCH (source: `Emission By Sector` { `CountryID`: row.`CountryID` })
  MATCH (target: `Sectors` { `Sector`: row.`Sector` })
  MERGE (source)-[r: `Emission sector`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_3) AS row
WITH row 
CALL {
  WITH row
  MATCH (source: `Emission By Sector` { `CountryID`: row.`CountryID` })
  MATCH (target: `Substance ` { `Substance`: row.`Substance` })
  MERGE (source)-[r: `Emission type`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_4) AS row
WITH row 
CALL {
  WITH row
  MATCH (source: `Total Emission` { `Substance`: row.`Substance` })
  MATCH (target: `Substance ` { `Substance`: row.`Substance` })
  MERGE (source)-[r: `Emission Type`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;
