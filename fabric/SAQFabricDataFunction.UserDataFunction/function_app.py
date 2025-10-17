import fabric.functions as fn
import logging

udf = fn.UserDataFunctions()

# Select 'Manage connections' and add a connection to a Fabric SQL Database 
# Replace the alias "<alias for sql database>" with your connection alias.
@udf.connection(argName="sqlDB",alias="SAQSQLDatabase")
@udf.function()
def read_cocktails(sqlDB: fn.FabricSqlConnection, alcools : list)-> list:
    '''
    Description: Read cocktails from SQL database using sample query.
    
    Args:
        sqlDB (fn.FabricSqlConnection): Fabric SQL database connection.
    
    Returns:
        list: Cocktails.
        
    Example:
        Returns 
    '''

    # Create placeholders for IN Clause
    placeholders = ','.join(['?'] * len(alcools))

    # Replace with the query you want to run
    query = f"""SELECT
                CD.NomCocktail,
                CD.Alcool,
                CD.[IngrédientsSupplémentaires],
                CD.[Préparation]
                FROM [saq].[CocktailDetails] CD
                INNER JOIN [saq].[Cocktails] COK ON COK.[IDCocktail] = CD.[IDCocktail]
                WHERE CD.[Alcool] IN ({placeholders});"""

    # Establish a connection to the SQL database
    connection = sqlDB.connect()
    cursor = connection.cursor()

    # Execute the query
    cursor.execute(query, alcools)

    # Fetch all results
    results = []
    for row in cursor.fetchall():
        results.append(row)

    # Close the connection
    cursor.close()
    connection.close()

    logging.info("read_cocktails successful executed.")
        
    return results

# Select 'Manage connections' and add a connection to a Fabric SQL Database 
# Replace the alias "<alias for sql database>" with your connection alias.
@udf.connection(argName="sqlDB",alias="SAQSQLDatabase")
@udf.function()
def read_stocks_in_shops(sqlDB : fn.FabricSqlConnection, alcools : list) -> list:
    '''
    Description: Read stocks in shops from SQL database using sample query.
    
    Args:
        sqlDB (fn.FabricSqlConnection): Fabric SQL database connection.
    
    Returns:
        list: Cocktails.
        
    Example:
        Returns 
    '''

    # Create placeholders for IN Clause
    placeholders = ','.join(['?'] * len(alcools))
    
    # Replace with the query you want to run
    query = f"""SELECT
                MAG.[NomMagasin],
                MAG.[VilleMagasin],
                SMAG.[QteStock],
                SMAG.[EtatStock],
                CD.[NomCocktail],
                CD.[Alcool],
                CD.[IngrédientsSupplémentaires],
                CD.[Préparation]
                FROM [saq].[CocktailDetails] CD
                LEFT JOIN [saq].[Cocktails] COK ON COK.[IDCocktail] = CD.[IDCocktail]
                LEFT JOIN [saq].[ListAlcools] LA ON LA.[IDAlcool] = COK.[IDAlcool] 
                LEFT JOIN [saq].[Stocks_Alcool_Magasins] SMAG ON SMAG.[AlcoolID] = LA.[IDAlcool]
                LEFT JOIN [saq].[Magasins] MAG ON MAG.[IDMagasin] = SMAG.[MagasinID]
                WHERE CD.[Alcool] IN ({placeholders}) AND smag.[QteStock] > 0;"""

    # Establish a connection to the SQL database
    connection = sqlDB.connect()
    cursor = connection.cursor()

    # Execute the query
    cursor.execute(query, alcools)

    # Fetch all results
    results = []
    for row in cursor.fetchall():
        results.append(row)

    # Close the connection
    cursor.close()
    connection.close()

    logging.info("read_stocks_in_shops successful executed.")
        
    return results

# Select 'Manage connections' and add a connection to a Fabric SQL Database 
# Replace the alias "<alias for sql database>" with your connection alias.
@udf.connection(argName="sqlDB",alias="SAQSQLDatabase")
@udf.function()
def write_alcool(sqlDB: fn.FabricSqlConnection, NomAlcool : str, TypeAlcool : str, Degré : str, Pays : str) -> str:
    '''
    Description: Insert one alcool into SQL database.
    
    Args:
        sqlDB (fn.FabricSqlConnection): Fabric SQL database connection.
        NomAlcool (str): Nom d'Alcool.
        TypeAlcool (str): Type d'Alcool.
        Degré (str): Degré d'Alcool.
        Pays (str): Pays de l'Alcool.
    
    Returns:
        str: Confirmation message about data insertion.
        
    '''

    # Replace with the data you want to insert
    data = (NomAlcool, TypeAlcool, Degré, Pays)

    # Establish a connection to the SQL database
    connection = sqlDB.connect()
    cursor = connection.cursor()
 
    # Insert data into the table
    insert_query = "INSERT INTO [saq].[ListAlcools] (IDAlcool, NomAlcool, TypeAlcool, Degré, Pays) VALUES (STR((SELECT MAX(CAST([IDAlcool] AS INTEGER)) + 1 FROM [saq].[ListALcools])), ?, ?, ?, ?);"
    cursor.execute(insert_query, data)

    # Commit the transaction
    connection.commit()

    # Close the connection
    cursor.close()
    connection.close()

    logging.info(f"The {NomAlcool} was added to ListAlcools.")

    return f"The {NomAlcool} was added to this table."

# Select 'Manage connections' and add a connection to a Fabric SQL Database 
# Replace the alias "<alias for sql database>" with your connection alias.
@udf.connection(argName="sqlDB",alias="SAQSQLDatabase")
@udf.function()
def write_cocktaildetails(sqlDB: fn.FabricSqlConnection, NomCocktail : str, Alcool : str, IDAlcool : str, IngrédientsSupplémentaires : str, Préparation : str) -> str:
    '''
    Description: Insert one cocktaildetails record into SQL database.
    
    Args:
        sqlDB (fn.FabricSqlConnection): Fabric SQL database connection.
        NomCocktail (str): Nom de Cocktail.
        IDAlcool (str): ID d'Alcool.
        IngrédientsSupplémentaires (str): Ingrédients Supplémentaires.
        Préparation (str): Préparation du Cocktail.
    
    Returns:
        str: Confirmation message about data insertion.
        
    '''

    # Replace with the data you want to insert
    data = (NomCocktail, Alcool, IDAlcool, IngrédientsSupplémentaires, Préparation)

    # Establish a connection to the SQL database
    connection = sqlDB.connect()
    cursor = connection.cursor()
 
    # Insert data into the table
    insert_query = "INSERT INTO [saq].[CocktailDetails] (IDCocktail, NomCocktail, Alcool, IDAlcool, IngrédientsSupplémentaires, Préparation) VALUES (STR((SELECT MAX(CAST([IDCocktail] AS INTEGER)) + 1 FROM [saq].[CocktailDetails])), ?, ?, ?, ?, ?);"
    cursor.execute(insert_query, data)

    # Commit the transaction
    connection.commit()

    # Close the connection
    cursor.close()
    connection.close()

    logging.info(f"The {NomCocktail} was added to NomCocktail.")

    return f"The {NomCocktail} was added to this table."

