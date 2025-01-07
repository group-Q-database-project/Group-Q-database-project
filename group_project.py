import psycopg2
import pandas as pd
from psycopg2 import sql

"""
    I used dataset only for one year 2020 - there were different questions for every year, so it didn't make sense
    I deleted bonuses - 1/3 answears didn't have value
    Additionally, I deleted rows with at least one NaN, in other case I would have to manually change all data 

    To use script, change user and password in DATABASE and conn_create in create_db() or in your postgreSQL
    as well as path_to_file to access .csv file
"""

# params for connection to postgresql 
DATABASE = {
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': 5432,
    'database': 'cityITdb' #db name
}

path_to_file = "/home/aleksandra/Pobrane/Databases/GroupProject/IT Salary Survey EU  2020.csv"

def create_db():
    conn_create = psycopg2.connect( #connection to default db 'postgres' to create new one
        host = "localhost",
        dbname = "postgres",
        user = "postgres",
        password = "postgres",
        port = 5432
    )

    conn_create.autocommit = True # only for creating database because it can't be done as a transaction
    cursor = conn_create.cursor()

    query = sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(DATABASE['database']))
    cursor.execute(query)
    query = sql.SQL("CREATE DATABASE {}").format(sql.Identifier(DATABASE['database']))
    cursor.execute(query)

    cursor.close()
    conn_create.close()

# connection to db in separate function
def connect_to_db():
    conn = psycopg2.connect(
        user = DATABASE['user'],
        password = DATABASE['password'],
        host = DATABASE['host'],
        port = DATABASE['port'],
        dbname = DATABASE['database']
    )
    return conn

# relational database
def create_tables():
    create_table_queries = [
        """ 
        CREATE TABLE IF NOT EXISTS City (
            City_ID SERIAL PRIMARY KEY,
            City_Name VARCHAR(50) NOT NULL UNIQUE
        );
        """, # multiline strings """
        """
        CREATE TABLE IF NOT EXISTS Position (
            Position_ID SERIAL PRIMARY KEY,
            Position_Name VARCHAR(50) NOT NULL UNIQUE
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS Technology (
            Technology_ID SERIAL PRIMARY KEY,
            Technology_Name VARCHAR(128) NOT NULL UNIQUE
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS Company (
            Company_ID SERIAL PRIMARY KEY,
            Company_Size VARCHAR(128) NOT NULL,
            Company_Type VARCHAR(128) NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS Employee (
            ID SERIAL PRIMARY KEY,
            Age INT NOT NULL,
            Gender VARCHAR(20) NOT NULL,
            City_ID INT NOT NULL REFERENCES City(City_ID), 
            Position_ID INT NOT NULL REFERENCES Position(Position_ID),
            Experience_Total numeric(3,1) NOT NULL,
            Seniority VARCHAR(20) NOT NULL,
            Technology_ID INT NOT NULL REFERENCES Technology(Technology_ID),
            Salary INT,
            Vacation_Days INT,
            Company_ID INT NOT NULL REFERENCES Company(Company_ID)
        );
        """ # REFERENCES - foreign key
    ]

    conn = connect_to_db()
    cursor = conn.cursor()
    for query in create_table_queries:
        cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()
    

def import_to_db(file): 
    use_columns = [
        "Age", "Gender", "City", "Position ", "Total years of experience", #"Position " originally with additional space at the end from dataset
        "Seniority level", "Your main technology / programming language",
        "Yearly brutto salary (without bonus and stocks) in EUR",
        "Number of vacation days",
        "Company size", "Company type"
    ]
    df = pd.read_csv(file, usecols=use_columns) # DataFrame from pandas with limited columns used

    df.rename(columns={ # in SQL column names cannot contain space 
        "City": "City_Name",
        "Position ": "Position_Name",
        "Total years of experience": "Experience_Total",
        "Seniority level": "Seniority",
        "Your main technology / programming language": "Technology_Name",
        "Yearly brutto salary (without bonus and stocks) in EUR": "Salary",
        "Number of vacation days": "Vacation_Days",
        "Company size": "Company_Size",
        "Company type": "Company_Type"
    }, inplace=True) # inplace=True modifies original df

    ### curing data
    """
        some rows contains a lot of NaN and some columns which should be numeric contains also strings
        rows with at least one wrong value are 17% of datasets so I decided to delete them
    """

    print("Number of all answears: ", len(df))
    print("Number of rows with at least one NaN: ", df.isnull().any(axis=1).sum())
    #print(df.isna().sum())
    #print(df['Experience_Total'].value_counts())
    #print(df['Vacation_Days'].value_counts())
    df['Experience_Total'] = pd.to_numeric(df['Experience_Total'], errors='coerce')
    df['Vacation_Days'] = pd.to_numeric(df['Vacation_Days'], errors='coerce')
    #print(df['Experience_Total'].value_counts())
    #print(df['Vacation_Days'].value_counts())
    print("Number of rows with at least one NaN and not numeric values in Experience_Total and Vacation_Days: ", df.isnull().any(axis=1).sum())
    df = df.dropna()
    print("Rows with NaN deleted. Number of answears:", len(df))

    #print(df['Seniority'].value_counts())
    seniority = ["Senior", "Middle", "Lead", "Junior", "Head"]
    df["Seniority"] = df["Seniority"].apply(lambda x: x if x in seniority else "Others")
    #print(df['Seniority'].value_counts())

    print(df['Technology_Name'].value_counts())

    conn = connect_to_db()
    cursor = conn.cursor()

    ### data ingestion
    for city in df["City_Name"].unique():
        cursor.execute("""
            INSERT INTO City (City_Name)
            VALUES (%s)
            ON CONFLICT (City_Name) DO NOTHING; 
        """, (city,))

    for position in df["Position_Name"].unique():
        cursor.execute("""
            INSERT INTO Position (Position_Name)
            VALUES (%s)
            ON CONFLICT (Position_Name) DO NOTHING;
        """, (position,))

    for technology in df["Technology_Name"].unique():
        cursor.execute("""
            INSERT INTO Technology (Technology_Name)
            VALUES (%s)
            ON CONFLICT (Technology_Name) DO NOTHING;            
        """, (technology,))

    for row in df[["Company_Size", "Company_Type"]].drop_duplicates().itertuples(index=False):
        cursor.execute("""
            INSERT INTO Company (Company_Size, Company_Type)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING;
            """, (row.Company_Size, row.Company_Type))

    conn.commit()
    
    # Employee table
    for row in df.itertuples(index=False):
        # determine IDs for Employee table
        cursor.execute("""
                    SELECT City_ID FROM City 
                    WHERE City_Name = %s
                       """, (row.City_Name,))
        city_id = cursor.fetchone()[0]

        cursor.execute("""
                    SELECT Position_ID FROM Position 
                    WHERE Position_Name = %s
                       """, (row.Position_Name,))
        position_id = cursor.fetchone()[0]

        cursor.execute("""
                    SELECT Technology_ID FROM Technology 
                    WHERE Technology_Name = %s
                       """, (row.Technology_Name,))
        technology_id = cursor.fetchone()[0]

        cursor.execute("""
                    SELECT Company_ID FROM Company
                    WHERE Company_Size = %s AND Company_Type = %s
                       """, (row.Company_Size, row.Company_Type))
        company_id = cursor.fetchone()[0]
        
        cursor.execute("""
                       INSERT INTO Employee (
                       Age, Gender, City_ID, Position_ID, Experience_Total, Seniority,
                       Technology_ID, Salary, Vacation_Days, Company_ID
                      ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                       """, (row.Age, row.Gender, city_id, position_id, row.Experience_Total,
                             row.Seniority, technology_id, row.Salary, row.Vacation_Days, company_id))

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    
    create_db()
    create_tables()
    import_to_db(path_to_file)
