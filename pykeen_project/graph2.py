import dotenv
import os
from neo4j import GraphDatabase

dotenv.load_dotenv("Neo4j-04a919b2-Created-2024-01-07.txt")

URI = os.getenv("NEO4J_URI")
AUTH = (os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD"))

with GraphDatabase.driver(URI, auth=AUTH) as driver:
    driver.verify_connectivity()
    
    
    
from pykeen.triples import TriplesFactory
import pandas as pd

# Define the paths to your Excel files for each node type
file_paths = {
    'Country': 'country.xlsx',
    'FossilCO2': 'fossil_CO2_by_country.xlsx',
    'GDP':'GDP.xlsx',
    'Population':'Population.xlsx',
    'Total_CO2_emission':'Total_CO2_emission.xlsx',
    'Monetary':'Monetary.xlsx'
    # Add other node types here with their respective file paths
}

# Initialize an empty list to store triples
all_triples = []

# Process each node type
for node_type, file_path in file_paths.items():
    # Read the Excel file using pandas
    df = pd.read_excel(file_path)
    
    if node_type == 'Country':
        # Extract the necessary columns from the 'Country' Excel file
        for index, row in df.iterrows():
            entity1 = row['countryID']
            
            entity3 = row['country']

            # Create a triple of the form (entity1, node_type, entity2, entity3)
            quadruple = [
                entity1, node_type, entity3,
            ]
            # Append the triple to the list of all triples
            all_triples.append(quadruple)
            
    elif node_type == 'FossilCO2':
        # Extract the necessary columns from the 'FossilCO2' Excel file
        for index, row in df.iterrows():
            # entity1 = row['Substance']
            entity2 = row['Sector']
            entity3 = row['CountryID']
            entity4 = row['Country']
            # Add more columns as needed (2013 to 2021)
            entity5 = row[2013]
            entity6 = row[2014]
            entity7 = row[2015]
            entity8 = row[2016]
            entity9 = row[2017]
            entity10 = row[2018]
            entity11 = row[2019]
            entity12 = row[2020]
            entity13 = row[2021]

            quadruple = [
                entity1, node_type, entity2, entity3, entity4,
                entity5, entity6, entity7, entity8, entity9,
                entity10, entity11, entity12, entity13
            ]

        # Extend the list of all triples with the new triples
        all_triples.append(quadruple)
            
    elif node_type == 'GDP':
        # Extract the necessary columns from the 'GDP' Excel file
        for index, row in df.iterrows():
            entity1 = row['monetary']
            entity2 = row['countryID']
            # Add more columns as needed (2013 to 2023)
            entity3 = row[2013]
            entity4 = row[2014]
            entity5 = row[2015]
            entity6 = row[2016]
            entity7 = row[2017]
            entity8 = row[2018]
            entity9 = row[2019]
            entity10 = row[2020]
            entity11 = row[2021]
            entity12 = row[2022]
            entity13 = row[2023]

            quadruple = [
                entity1, node_type, entity2, entity3, entity4,
                entity5, entity6, entity7, entity8, entity9,
                entity10, entity11, entity12, entity13
            ]

        # Extend the list of all triples with the new triples
        all_triples.append(quadruple)
            
    elif node_type == 'Population':
    # Extract the necessary columns from the 'Population' Excel file
        for index, row in df.iterrows():
            entity1 = row['countryID']
            # Add more columns as needed (2013 to 2023)
            entity2 = row[2013]
            entity3 = row[2014]
            entity4 = row[2015]
            entity5 = row[2016]
            entity6 = row[2017]
            entity7 = row[2018]
            entity8 = row[2019]
            entity9 = row[2020]
            entity10 = row[2021]
            entity11 = row[2022]
            entity12 = row[2023]

            quadruple = [
                entity1, node_type, entity2, entity3, entity4,
                entity5, entity6, entity7, entity8, entity9,
                entity10, entity11, entity12
            ]

        # Extend the list of all triples with the new triples
        all_triples.append(quadruple)
            
        
    elif node_type == 'Total_CO2_emission':
        # Extract the necessary columns from the 'Total CO2 emission' Excel file
        for index, row in df.iterrows():
            entity1 = row['Substance']
            # entity2 = row["Total"]
            # Add more columns as needed (yr2013 to yr2021)
            entity3 = row['yr2013']
            entity4 = row['yr2014']
            entity5 = row['yr2015']
            entity6 = row['yr2016']
            entity7 = row['yr2017']
            entity8 = row['yr2018']
            entity9 = row['yr2019']
            entity10 = row['yr2020']
            entity11 = row['yr2021']

            # Create a quadruple of the form (entity1, node_type, entity2, entity3, ... , entity10)
            quadruple = [
                entity1, node_type, entity3, entity4,
                entity5, entity6, entity7, entity8, entity9,
                entity10, entity11
            ]

        # Extend the list of all triples with the new triples
        all_triples.append(quadruple)
   
    elif node_type == 'Monetary':
        # Extract the necessary columns from the 'Monetary' Excel file
        for index, row in df.iterrows():
            entity1 = row['monetary']
            entity2 = row['countryID']
            # Add more columns as needed (yr2013 to yr2023)
            entity3 = row['yr2013']
            entity4 = row['yr2014']
            entity5 = row['yr2015']
            entity6 = row['yr2016']
            entity7 = row['yr2017']
            entity8 = row['yr2018']
            entity9 = row['yr2019']
            entity10 = row['yr2020']
            entity11 = row['yr2021']
            entity12 = row['yr2022']
            entity13 = row['yr2023']

            # Create a quadruple of the form (entity1, node_type, entity2, entity3, ... , entity13)
            quadruple = [
                entity1, node_type, entity2, entity3, entity4,
                entity5, entity6, entity7, entity8, entity9,
                entity10, entity11, entity12, entity13
            ]
            # Append the quadruple to the list of all triples
            all_triples.append(quadruple)
 
import numpy as np
triples_array = np.array(all_triples)   
# Create a TriplesFactory and load the triples into PyKEEN
triples_factory = TriplesFactory.from_labeled_triples(triples=triples_array)

# Now 'triples_factory' contains all your nodes (Country, FossilCO2) in PyKEEN format









# similarly now retrive relationships of all nodes and store in in pykeen

# Define an empty list to store triples
all_triples = []

with GraphDatabase.driver(URI, auth=AUTH) as driver:
    # Create a Neo4j session
    with driver.session() as session:
        # Define the queries
        queries = [
            "MATCH p=()-[:Economy]->() RETURN nodes(p)[0] AS head_entity, 'Economy' AS relation, nodes(p)[1] AS tail_entity LIMIT 25;",
            "MATCH p=()-[:`Emission in sector`]->() RETURN nodes(p)[0] AS head_entity, 'Emission in sector' AS relation, nodes(p)[1] AS tail_entity LIMIT 25;",
            "MATCH p=()-[:`Emission sector`]->() RETURN nodes(p)[0] AS head_entity, 'Emission sector' AS relation, nodes(p)[1] AS tail_entity LIMIT 25;",
            "MATCH p=()-[:`Emission type`]->() RETURN nodes(p)[0] AS head_entity, 'Emission type' AS relation, nodes(p)[1] AS tail_entity LIMIT 25;",
            "MATCH p=()-[:`Emission Type`]->() RETURN nodes(p)[0] AS head_entity, 'Emission Type' AS relation, nodes(p)[1] AS tail_entity LIMIT 25;",
            "MATCH p=()-[:`GDP of`]->() RETURN nodes(p)[0] AS head_entity, 'GDP of' AS relation, nodes(p)[1] AS tail_entity LIMIT 25;",
            "MATCH p=()-[:GDPperCapita]->() RETURN nodes(p)[0] AS head_entity, 'GDPperCapita' AS relation, nodes(p)[1] AS tail_entity LIMIT 25;",
            "MATCH p=()-[:Location]->() RETURN nodes(p)[0] AS head_entity, 'Location' AS relation, nodes(p)[1] AS tail_entity LIMIT 25;",
            "MATCH p=()-[:`Total Emission by sector`]->() RETURN nodes(p)[0] AS head_entity, 'Total Emission by sector' AS relation, nodes(p)[1] AS tail_entity LIMIT 25;",
            # Add other queries similarly...
        ]

        # Iterate over queries
        for query in queries:
            # Query Neo4j to retrieve relationships
            result = session.run(query)

            # Iterate over the Neo4j query results
            for record in result:
                # Extract relationship information from Neo4j record
                head_entity = record['head_entity']
                relation = record['relation']
                tail_entity = record['tail_entity']

                # Create a triple of the form (head_entity, relation, tail_entity)
                triple = (head_entity, relation, tail_entity)

                # Append the triple to the list of all triples
                all_triples.append(triple)

# Create a TriplesFactory and load the triples into PyKEEN
triples_factory = TriplesFactory.from_list(triples=all_triples)





# Now at last choose your model i choose TransE
from pykeen.pipeline import pipeline

# Replace this with the path to the folder where you have your training triples
data_directory = 'D:\task\task2'

# Define the pipeline configuration
result = pipeline(
    training=data_directory,
    model='TransE',
    random_seed=1234,  # Set your desired random seed
    training_kwargs=dict(num_epochs=100),  # You can modify training settings here
    evaluation_kwargs=dict(),
    model_kwargs=dict(embedding_dim=50),  # You can set the embedding dimension here
)

# The result contains trained model, losses, and other information
trained_model = result.model