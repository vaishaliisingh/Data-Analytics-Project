# Databricks notebook source
df=spark.read.csv("dbfs:/FileStore/shared_uploads/avaishu2003@gmail.com/sales_data.csv",header=True)
# df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/avaishu2003@gmail.com/sales_data.csv")

# COMMAND ----------

df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md #Creating the database for SQL queries

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS sales_db")

# COMMAND ----------

# MAGIC %md ### Using the same database for all transaction

# COMMAND ----------

spark.sql("use sales_db")

# COMMAND ----------

# DBTITLE 1,Creating table, columns using spark SQL component
spark.sql(""" CREATE TABLE IF NOT EXISTS sales3(
            OrderId STRING,
            Product STRING,
            QuantityOrdered STRING,
            PriceEach STRING,
            OrderDate STRING,
            PurchaseAddress STRING
)""" ) 
         

# COMMAND ----------

spark.sql("select * from sales3").show(1)

# COMMAND ----------

# DBTITLE 1,DataFrame can be converted to table for running sql queries
df.createOrReplaceTempView("temp_sales")

# COMMAND ----------

spark.sql("select * from temp_sales").show(4)

# COMMAND ----------

# MAGIC %md #### Inserting record using the subquery - into sales_raw table

# COMMAND ----------

# second way   insert overwrite table select `col1`,`col2` from table

spark.sql(""" INSERT OVERWRITE sales3
              SELECT * FROM temp_sales""")

# COMMAND ----------

spark.sql("select * from sales3").show(1)

# COMMAND ----------

# DBTITLE 1,Running sql queries on the table  %sql ( magic commands)
# MAGIC %sql
# MAGIC select * from sales3 limit 1;

# COMMAND ----------

# MAGIC %md #Data Cleansing for the project

# COMMAND ----------

# MAGIC %md ##### (Bad Data) Record which are bad for the quality analysis - Have a look on the same

# COMMAND ----------

# DBTITLE 1,Checking null value in data
# MAGIC %sql
# MAGIC select * from sales3
# MAGIC where OrderId="null"
# MAGIC limit 5;

# COMMAND ----------

df.na.drop().show()

# COMMAND ----------

# MAGIC %md  ## More issue with my issues ( order Id, product)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from sales3
# MAGIC where orderID="Order ID" limit 5;

# COMMAND ----------

# MAGIC %md ### Way to filter bad data [ Null value, orderId in data]

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from sales2
# MAGIC where orderID != "Order ID" and orderid!="null";

# COMMAND ----------

# MAGIC %md ## CREATING TEMP table to check whether we get data perfectly or not

# COMMAND ----------

# MAGIC %sql
# MAGIC with tmp_sale as (
# MAGIC   select * from sales2
# MAGIC where orderID != "Order ID" and orderid!="null"
# MAGIC )
# MAGIC
# MAGIC
# MAGIC select * from tmp_sale 
# MAGIC where orderID = "null";

# COMMAND ----------

# MAGIC %md ### Split and checking the columns - With Filter

# COMMAND ----------

# MAGIC %sql
# MAGIC select split(PurchaseAddress,',') as City
# MAGIC   from sales2
# MAGIC     where orderID != "Order ID" and orderid!="null"

# COMMAND ----------

# MAGIC %md #### extract only the city fields based on index

# COMMAND ----------

# MAGIC %sql
# MAGIC select split(PurchaseAddress,',')[1] as City
# MAGIC   from sales2
# MAGIC     where orderID != "Order ID" and orderid!="null"

# COMMAND ----------

# MAGIC %md ##Now extracting the city based on substr method [City, State]

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select split(PurchaseAddress,',')[1] as City,
# MAGIC   split(PurchaseAddress,',')[2] as State
# MAGIC   from sales2
# MAGIC     where orderID != "Order ID" and orderid!="null"

# COMMAND ----------

# DBTITLE 1,Only taking data based on string : we pass first argument as string in substr
# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC select split(PurchaseAddress,',')[1] as City,
# MAGIC   substr( split(PurchaseAddress,',')[2], 2,2 )as State
# MAGIC   from sales2
# MAGIC     where orderID != "Order ID" and orderid!="null" limit 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC desc sales2;

# COMMAND ----------

# MAGIC %md #Getting & preaparing column as per the requirement

# COMMAND ----------

# MAGIC %sql 
# MAGIC select cast(OrderId as int) as OrderId,
# MAGIC   product,
# MAGIC   QuantityOrdered,
# MAGIC   PriceEach,
# MAGIC   to_timestamp(OrderDate,'MM/dd/yy HH:mm') as OrderDate,
# MAGIC   PurchaseAddress,
# MAGIC   split(PurchaseAddress,',')[1] as City,
# MAGIC   substr( split(PurchaseAddress,',')[2],2,2) as State,
# MAGIC   year( to_timestamp(OrderDate,'MM/dd/yy HH:mm') ) as ReportYear,
# MAGIC   month(to_timestamp(OrderDate,'MM/dd/yy HH:mm') ) as Month
# MAGIC   
# MAGIC   from sales2
# MAGIC   where orderID != "Order ID" and orderid!="null" limit 3
# MAGIC   
# MAGIC   
# MAGIC   

# COMMAND ----------

# DBTITLE 1,You can also create a data frame from your SQL table
temp_sale_df = spark.sql(""" 
          select cast(OrderId as int) as OrderId,
                  product,
                  QuantityOrdered,
                  PriceEach,
                  to_timestamp(OrderDate,'MM/dd/yy HH:mm') as OrderDate,
                  PurchaseAddress,
                  split(PurchaseAddress,',')[1] as City,
                  substr( split(PurchaseAddress,',')[2],2,2) as State,
                  year( to_timestamp(OrderDate,'MM/dd/yy HH:mm') ) as ReportYear,
                  month(to_timestamp(OrderDate,'MM/dd/yy HH:mm') ) as Month
      from sales2
      where orderID != "Order ID" and orderid!="null";
    """)

# COMMAND ----------

# DBTITLE 1,Creating temp table for referencing data later
temp_sale_df.createOrReplaceTempView('tmp_sales')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tmp_sales limit 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table sales;

# COMMAND ----------

# DBTITLE 1,Creating table for the last work


spark.sql("""create table if not exists sales(
    OrderId INT,
    product STRING,
    Quantity INT,
    priceeach int,
    OrderDate TIMESTAMP,
    StoreAddress STRING,
    City STRING,
    State STRING,
    ReportYear INT,
    months INT
    )
  USING PARQUET
  PARTITIONED BY(ReportYear,months)
  options('compression'='snappy')
  location 'dbfs:/FileStore/salesdata3/published'
  
  """)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from tmp_sales;

# COMMAND ----------

# DBTITLE 1,Inserting data into newly created table using the virtual table tmp_sales
spark.sql("""INSERT INTO sales
    select OrderId,
            product,
            cast( QuantityOrdered as int),
            cast( PriceEach as int),
            OrderDate,
            PurchaseAddress,
            City,
            State,
            ReportYear,
            Month
            from tmp_sales
            
    
""")

# COMMAND ----------

# MAGIC %sql 
# MAGIC desc sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from sales;

# COMMAND ----------

# MAGIC %md #Project Analytics with case scenerios

# COMMAND ----------

# MAGIC %sql
# MAGIC use sales_db;
# MAGIC show tables;

# COMMAND ----------

# MAGIC %md #### Q1. Which is the best month of the sales done?  [ sales=price*quantity ]

# COMMAND ----------

# MAGIC %sql
# MAGIC select orderid,months,priceeach, quantity, (priceeach*quantity) as sales
# MAGIC from sales limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select months, round(sum( (priceeach * quantity) ),2) as TotalSales
# MAGIC from sales
# MAGIC group by months
# MAGIC order by months asc;

# COMMAND ----------

# DBTITLE 1,Solution : case scenerio
# MAGIC %sql
# MAGIC
# MAGIC select months, round(sum( (priceeach * quantity) ),2) as TotalSales
# MAGIC from sales
# MAGIC group by months
# MAGIC order by months asc;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select city,sum(quantity) as totalProducts
# MAGIC from sales
# MAGIC group by city
# MAGIC order by totalProducts desc
# MAGIC

# COMMAND ----------

# MAGIC %md ### Q2. Which city sold the most products

# COMMAND ----------

# MAGIC %sql
# MAGIC select city,sum(quantity) as totalProducts
# MAGIC from sales
# MAGIC group by city
# MAGIC order by totalProducts desc

# COMMAND ----------

# MAGIC %md ## Q3. what is best time for the advertisment to maximize the sale of my product

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct orderId,
# MAGIC   hour(orderDate) as hour
# MAGIC   from sales

# COMMAND ----------

# MAGIC %sql 
# MAGIC with tmp_orders as (
# MAGIC   select distinct orderId,
# MAGIC   hour(orderDate) as hour
# MAGIC   from sales
# MAGIC )
# MAGIC
# MAGIC
# MAGIC -- order to be calculated
# MAGIC select hour,count(orderId) as totalOrder
# MAGIC from tmp_orders group by hour
# MAGIC order by hour asc;

# COMMAND ----------

# MAGIC %sql
# MAGIC desc sales;

# COMMAND ----------

# MAGIC %sql 
# MAGIC with abc as(
# MAGIC   select distinct orderId,
# MAGIC   hour(orderDate) as hour
# MAGIC   from sales)
# MAGIC
# MAGIC select * from abc;
# MAGIC

# COMMAND ----------

# DBTITLE 1,We NEED TO KNOW NUMBER OF ORDER PLACE IN EACH HOUR
# MAGIC %sql 
# MAGIC with tmp_orders as (
# MAGIC   select distinct orderId,
# MAGIC   hour(orderDate) as hour
# MAGIC   from sales
# MAGIC )
# MAGIC
# MAGIC
# MAGIC -- order to be calculated
# MAGIC select hour,count(orderId) as totalOrder
# MAGIC from tmp_orders group by hour
# MAGIC order by hour asc;
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md ### Q4. Most ofter product sold together in State like NY ( we are talking about 2 products at same time)
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC select orderId,
# MAGIC count(1) as orderIds
# MAGIC   from sales
# MAGIC     where State= ' NY'
# MAGIC     group by orderId
# MAGIC     having count(1) >1;
# MAGIC     

# COMMAND ----------

# DBTITLE 1,Collect_list() function to combine for every order_id
#  In the above case we have seen orderId 304276 is linked with two or more products
#  we will work to combine those 2 or more product together liked to orderId
# also will get the size to know how many products are linked with each orderId

# COMMAND ----------

# MAGIC %sql
# MAGIC select orderId,
# MAGIC state,
# MAGIC collect_list(product) as ProductList,
# MAGIC size(collect_list(product)) as ListSize
# MAGIC from sales
# MAGIC where State= 'NY'
# MAGIC     group by orderId,state
# MAGIC     having size(collect_list(product)) >1;

# COMMAND ----------

# DBTITLE 1,Read this
# we need to group the similar kind of productList together as we need to find how many product are there
#  see the same product list of 141365 $ 141910

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc sales;

# COMMAND ----------

# DBTITLE 1,Solution : case scenario 4
# MAGIC %sql
# MAGIC
# MAGIC with tmp_prd as (
# MAGIC select orderid,
# MAGIC   product,state
# MAGIC   from sales
# MAGIC   where state='NY'
# MAGIC order by product
# MAGIC ),
# MAGIC
# MAGIC --  creating second temp table for the ordered product list for the order product from above temp table
# MAGIC tmp_product_list as (
# MAGIC select orderId,
# MAGIC   state,
# MAGIC   collect_list(product) as ProductList,
# MAGIC   size(collect_list(product)) as ListSize
# MAGIC   from tmp_prd
# MAGIC   where State= 'NY'
# MAGIC       group by orderId,state)
# MAGIC       
# MAGIC       
# MAGIC select productList,count(1) as count from tmp_product_list
# MAGIC where ListSize>1
# MAGIC group by productList order by count desc limit 5
