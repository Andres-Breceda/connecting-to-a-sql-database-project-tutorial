import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

# Cargar el archivo .env con las credenciales
load_dotenv("/workspaces/connecting-to-a-sql-database-project-tutorial/.env.newdb")

# Obtener credenciales desde variables de entorno
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
print(f"Usuario: {db_user}, Contraseña: {db_password}, Host: {db_host}")  # Verifica que el archivo se cargó correctamente

# Nombre de la base de datos
db_name = "andres0114"

# Conectar a PostgreSQL
engine = create_engine(f"postgresql://{db_user}:{db_password}@{db_host}/postgres", isolation_level="AUTOCOMMIT")

# Crear la base de datos si no existe
with engine.connect() as connection:
    try:
        # Intentar crear la base de datos
        connection.execute(f"CREATE DATABASE {db_name}")
        print(f"Base de datos '{db_name}' creada exitosamente.")
    except Exception as e:
        # Si la base de datos ya existe, manejar el error sin mostrarlo
        if 'already exists' in str(e).lower():
            print(f"La base de datos '{db_name}' ya existe.")

# Crear el motor para la base de datos recién creada
engine = create_engine(f"postgresql://{db_user}:{db_password}@{db_host}/{db_name}")

Base = declarative_base()

# Definir las clases (tablas)
class Publisher(Base):
    __tablename__ = 'publishers'

    id_publisher = Column(Integer, primary_key=True)
    name = Column(String)

    def __repr__(self):
        return f"<Publisher(id_publisher={self.id_publisher}, name={self.name})>"

class Author(Base):
    __tablename__ = 'authors'

    id_authors = Column(Integer, primary_key=True)
    first_name = Column(String, nullable=True)
    middle_name = Column(String, nullable=True)
    last_name = Column(String, nullable=False)

    def __repr__(self):
        return f"<Author(id_authors={self.id_authors}, first_name={self.first_name}, last_name={self.last_name})>"

class Book(Base):
    __tablename__ = 'books'

    id_book = Column(Integer, primary_key=True)
    title = Column(String, nullable=False)
    total_pages = Column(Integer, nullable=False)
    rating = Column(Integer, nullable=True)
    isbn = Column(String, nullable=True)
    published_date = Column(String, nullable=False)
    id_publisher = Column(Integer, ForeignKey('publishers.id_publisher'))

    publisher = relationship('Publisher', backref='books')

    def __repr__(self):
        return f"<Book(id_book={self.id_book}, title={self.title}, rating={self.rating})>"

class BookAuthor(Base):
    __tablename__ = 'book_authors'

    id_book = Column(Integer, ForeignKey('books.id_book'), primary_key=True)
    author_id = Column(Integer, ForeignKey('authors.id_authors'), primary_key=True)

    book = relationship('Book', backref='authors')
    author = relationship('Author', backref='books')

# Crear las tablas en la base de datos si no existen
Base.metadata.create_all(engine)

# Crear la sesión
Session = sessionmaker(bind=engine)
session = Session()

# Insertar datos en publishers
publishers_data = [
    Publisher(id_publisher=1, name='O Reilly Media'),
    Publisher(id_publisher=2, name='A Book Apart'),
    Publisher(id_publisher=3, name='A K PETERS'),
    Publisher(id_publisher=4, name='Academic Press'),
    Publisher(id_publisher=5, name='Addison Wesley'),
    Publisher(id_publisher=6, name='Albert&Sweigart'),
    Publisher(id_publisher=7, name='Alfred A. Knopf')
]

# Insertar datos en authors
authors_data = [
    Author(id_authors=1, first_name='Merritt', middle_name=None, last_name='Eric'),
    Author(id_authors=2, first_name='Linda', middle_name=None, last_name='Mui'),
    Author(id_authors=3, first_name='Alecos', middle_name=None, last_name='Papadatos'),
    Author(id_authors=4, first_name='Anthony', middle_name=None, last_name='Molinaro'),
    Author(id_authors=5, first_name='David', middle_name=None, last_name='Cronin'),
    Author(id_authors=6, first_name='Richard', middle_name=None, last_name='Blum'),
    Author(id_authors=7, first_name='Yuval', middle_name='Noah', last_name='Harari'),
    Author(id_authors=8, first_name='Paul', middle_name=None, last_name='Albitz')
]

# Insertar datos en books
books_data = [
    Book(id_book=1, title='Lean Software Development: An Agile Toolkit', total_pages=240, rating=4.17, isbn='9780320000000', published_date='2003-05-18', id_publisher=5),
    Book(id_book=2, title='Facing the Intelligence Explosion', total_pages=91, rating=3.87, isbn=None, published_date='2013-02-01', id_publisher=7),
    Book(id_book=3, title='Scala in Action', total_pages=419, rating=3.74, isbn='9781940000000', published_date='2013-04-10', id_publisher=1),
    Book(id_book=4, title='Patterns of Software: Tales from the Software Community', total_pages=256, rating=3.84, isbn='9780200000000', published_date='1996-08-15', id_publisher=1),
    Book(id_book=5, title='Anatomy Of LISP', total_pages=446, rating=4.43, isbn='9780070000000', published_date='1978-01-01', id_publisher=3),
    Book(id_book=6, title='Computing machinery and intelligence', total_pages=24, rating=4.17, isbn=None, published_date='2009-03-22', id_publisher=4),
    Book(id_book=7, title='XML: Visual QuickStart Guide', total_pages=269, rating=3.66, isbn='9780320000000', published_date='2009-01-01', id_publisher=5),
    Book(id_book=8, title='SQL Cookbook', total_pages=595, rating=3.95, isbn='9780600000000', published_date='2005-12-01', id_publisher=7),
    Book(id_book=9, title='The Apollo Guidance Computer: Architecture And Operation (Springer Praxis Books / Space Exploration)', total_pages=439, rating=4.29, isbn='9781440000000', published_date='2010-07-01', id_publisher=6),
    Book(id_book=10, title='Minds and Computers: An Introduction to the Philosophy of Artificial Intelligence', total_pages=222, rating=3.54, isbn='9780750000000', published_date='2007-02-13', id_publisher=7)
]

# Insertar datos en book_authors
book_authors_data = [
    BookAuthor(id_book=1, author_id=1),
    BookAuthor(id_book=2, author_id=8),
    BookAuthor(id_book=3, author_id=7),
    BookAuthor(id_book=4, author_id=6),
    BookAuthor(id_book=5, author_id=5),
    BookAuthor(id_book=6, author_id=4),
    BookAuthor(id_book=7, author_id=3),
    BookAuthor(id_book=8, author_id=2),
    BookAuthor(id_book=9, author_id=4),
    BookAuthor(id_book=10, author_id=1)
]

# Insertar los datos en la base de datos
session.add_all(publishers_data)
session.add_all(authors_data)
session.add_all(books_data)
session.add_all(book_authors_data)

# Confirmar la transacción
session.commit()

print("Datos insertados correctamente.")

# Cerrar la sesión
session.close()


    
