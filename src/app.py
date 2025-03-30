import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv


# Load environment variables
load_dotenv()

db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")

print(f"Usuario: {db_user}, Contraseña: {db_password}")

# 1) Connect to the database with SQLAlchemy
def connect():
    global engine
    try:
        connection_string = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
        print("Starting the connection...")
        engine = create_engine(connection_string, isolation_level="AUTOCOMMIT")
        engine.connect()
        print("Connected successfully!")
        return engine
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None

engine = connect()

if engine is None:
    exit()

# 2) Create the tables
with engine.connect() as connection:
    connection.execute(text("""
    -- publishers
    INSERT INTO publishers(id_publisher, name) VALUES
    (1, 'O Reilly Media'),
    (2, 'A Book Apart'),
    (3, 'A K PETERS'),
    (4, 'Academic Press'),
    (5, 'Addison Wesley'),
    (6, 'Albert&Sweigart'),
    (7, 'Alfred A. Knopf');

    -- authors 
    INSERT INTO authors (id_authors, first_name, middle_name, last_name) VALUES 
    (1, 'Merritt', null, 'Eric'),
    (2, 'Linda', null, 'Mui'),
    (3, 'Alecos', null, 'Papadatos'),
    (4, 'Anthony', null, 'Molinaro'),
    (5, 'David', null, 'Cronin'),
    (6, 'Richard', null, 'Blum'),
    (7, 'Yuval', 'Noah', 'Harari'),
    (8, 'Paul', null, 'Albitz');

    -- books
    INSERT INTO books (id_book, title, total_pages, rating, isbn, published_date, id_publisher) VALUES 
    (1, 'Lean Software Development: An Agile Toolkit', 240, 4.17, '9780320000000', '2003-05-18', 5),
    (2, 'Facing the Intelligence Explosion', 91, 3.87, null, '2013-02-01', 7),
    (3, 'Scala in Action', 419, 3.74, '9781940000000', '2013-04-10', 1),
    (4, 'Patterns of Software: Tales from the Software Community', 256, 3.84, '9780200000000', '1996-08-15', 1),
    (5, 'Anatomy Of LISP', 446, 4.43, '9780070000000', '1978-01-01', 3),
    (6, 'Computing machinery and intelligence', 24, 4.17, null, '2009-03-22', 4),
    (7, 'XML: Visual QuickStart Guide', 269, 3.66, '9780320000000', '2009-01-01', 5),
    (8, 'SQL Cookbook', 595, 3.95, '9780600000000', '2005-12-01', 7),
    (9, 'The Apollo Guidance Computer: Architecture And Operation (Springer Praxis Books / Space Exploration)', 439, 4.29, '9781440000000', '2010-07-01', 6),
    (10, 'Minds and Computers: An Introduction to the Philosophy of Artificial Intelligence', 222, 3.54, '9780750000000', '2007-02-13', 7);

    -- book authors
    INSERT INTO book_authors (id_book, author_id) VALUES 
    (1, 1),
    (2, 8),
    (3, 7),
    (4, 6),
    (5, 5),
    (6, 4),
    (7, 3),
    (8, 2),
    (9, 4),
    (10, 1);
    """))
     # 4) Use Pandas to read and display a table      
# Crear una conexión a SQLite (puede ser otra base de datos como PostgreSQL o MySQL)
engine = create_engine('sqlite:///mi_base_de_datos.db')

# Leer datos de una tabla SQL y almacenarlos en un DataFrame de Pandas
df = pd.read_sql("SELECT * FROM usuarios", engine)

print(df.head())  # Mostrar los primeros registros                 

engine.dispose()


