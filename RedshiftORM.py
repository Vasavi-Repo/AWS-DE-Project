from sqlalchemy import Sequence, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

# Redshift connection string
redshift_url = 'postgresql+psycopg2://admin:SaiRadha%2423@myfirstworkgroup.920372998244.ap-south-1.redshift-serverless.amazonaws.com:5439/dev'

# Create SQLAlchemy engine
engine = create_engine(redshift_url)
# Create a session to interact with the database
Session = sessionmaker(bind=engine)
session = Session()
from sqlalchemy import Column, Integer, String, Float, Date
from sqlalchemy.orm import declarative_base
Base = declarative_base()
create_sales_fact_table = """
CREATE TABLE IF NOT EXISTS sales_fact (
    sale_id INT IDENTITY(1,1) PRIMARY KEY,
    product_id INT,
    customer_id INT,
    quantity_sold INT,
    total_price FLOAT,
    sale_date DATE
);
"""

create_product_dim_table = """
CREATE TABLE IF NOT EXISTS product_dim (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(255),
    category VARCHAR(255),
    price FLOAT
);
"""

create_customer_dim_table = """
CREATE TABLE IF NOT EXISTS customer_dim (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(255),
    email VARCHAR(255),
    address VARCHAR(255)
);
"""

create_date_dim_table = """
CREATE TABLE IF NOT EXISTS date_dim (
    date_id INT PRIMARY KEY,
    date DATE,
    year INT,
    quarter INT,
    month INT,
    day INT
);
"""

# Execute the raw SQL
try:
    with engine.connect() as connection:
        connection.execute(create_sales_fact_table)
        connection.execute(create_product_dim_table)
        connection.execute(create_customer_dim_table)
        connection.execute(create_date_dim_table)
    print("Tables created successfully.")
except SQLAlchemyError as e:
    print(f"Error creating tables: {e}")
class CustomerDim(Base):
    __tablename__ = 'customer_dim'
    
    # Define columns
    customer_id = Column(Integer, primary_key=True)  # auto-generated in Redshift with IDENTITY
    customer_name = Column(String)
    email = Column(String)
    address = Column(String)
    
new_customer = CustomerDim(
    customer_id=1,
    customer_name="John Doe",
    email="johndoe@example.com",
    address="1234 Elm Street, Some City, ABC"
)

# Add to session and commit
session.add(new_customer)

try:
    session.commit()  # Commit the transaction
    print("Customer added successfully.")
except Exception as e:
    session.rollback()  # Rollback if any error occurs
    print(f"Error adding customer: {e}")
finally:
    session.close()
    # Query a specific customer by name and email
customer = session.query(CustomerDim).filter(
    CustomerDim.customer_name == 'John Doe',
    CustomerDim.email == 'johndoe@example.com'
).first()

# Print the result
if customer:
    print(f"Customer ID: {customer.customer_id}, Name: {customer.customer_name}, Email: {customer.email}")
else:
    print("Customer not found.")
