# Importing Necessary modules
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count
from pyspark.sql.functions import first, explode
from pyspark.sql.functions import round, when, dayofweek
from pyspark.sql.functions import col, to_date
from pyspark.sql.functions import split, udf, concat_ws
from pyspark.ml.feature import StopWordsRemover
from pyspark.sql.types import ArrayType, StringType, IntegerType,  StructType, \
                    StructField, DoubleType


from src import constants

# <------------------------------- LOAD DATA --------------------------------------------->
def load_data_from_mysql(spark :pyspark.sql.SparkSession, db_name :str, table_name :str) \
                        -> pyspark.sql.DataFrame:
    '''
        – load data from MySQL table 'table_name' from database 'db_name' and return the 
          table as a spark dataframe

        For the mysql connection use below information:
            jdbc driver: 'com.mysql.cj.jdbc.Driver'
            Hostname: 'localhost'
            Port: 3306
            Database: 'classicmodels'
            Table_name: 'order_details'
            Username: 'root'
            Password: 'pass@word1'
    '''

    spark_df = spark.read \
        .format("jdbc") \
        .option("driver","com.mysql.cj.jdbc.Driver") \
        .option("url", "jdbc:mysql://localhost:3306/"+db_name) \
        .option("dbtable", table_name) \
        .option("user", "root") \
        .option("password", "pass@word1") \
        .load()
    # print(spark_df.show(truncate=False))
    return spark_df

def load_data_from_csv(spark :pyspark.sql.SparkSession, csv_file_name: str) \
                      -> pyspark.sql.DataFrame:
    '''
    Load data from CSV file 'csv_file_name' and return a spark Dataframe
    PS: The data files for this assignment are in 'data' folder
    You can access full path of 'data/' folder using 'DATA_FOLDER' variable
    from constants.py
    ''' 
    return spark.read.csv(constants.DATA_FOLDER + csv_file_name, header=True, inferSchema=True)

def load_data_from_flatfile(spark :pyspark.sql.SparkSession, txt_file_name: str) \
                      -> pyspark.sql.DataFrame:
    '''
    Load data from flat file 'txt_file_name' separated with ':' and return a spark Dataframe
    PS: The data files for this assignment are in 'data' folder
    You can access full path of 'data/' folder using 'DATA_FOLDER' variable
    from constants.py
    '''
    return spark.read.csv(constants.DATA_FOLDER + txt_file_name, sep=':', header=True, inferSchema=True)


# <------------------------------- TRANSFORMATIONS --------------------------------------------->
def clean_product_MSRP_column(spark :pyspark.sql.SparkSession) -> pyspark.sql.DataFrame:
    '''
    Due to a data entry issues MSRP, the selling price is lower than its buyPrice for some
    products. Change MSRP to 1.4 times the buyPrice for such products and cast it to two
    decimal places.
    
    Return a spark dataframe with following columns.
    |productCode|productName|productLine|productScale|productVendor|productDescription|quantityInStock|buyPrice|MSRP|
    '''

    products = load_data_from_csv(spark, "products.csv")
    products = products.withColumn("MSRP", when(col("MSRP") < col("buyPrice"), round(1.4*col("buyPrice"), 2)) \
               .otherwise(col("MSRP")))
    return products

def clean_productlines_description(spark :pyspark.sql.SparkSession) -> pyspark.sql.DataFrame:
    '''
    Take testDescription from productLines table and clean it.
    1. Convert to lower-case
    2. Remove all special characters except a-z and 0-9.
    3. Remove stop words (using StopWordsRemover.loadDefaultStopWords("english"))
    The final value(testDescription) must be a string.
    
    Return a spark dataframe with following columns.
    |productLine|htmlDescription|image|textDescription|
    '''
    
    def remove_special_chars(words):
        return [word.lower().replace("[^a-z0-9 ]", "") for word in words]
    
    pLines = load_data_from_csv(spark, "productlines.csv")

    pLines = pLines.withColumn("textDescription", split(col("textDescription"), " "))
    remove_special_chars_udf = udf(remove_special_chars, ArrayType(StringType()))
    pLines = pLines.withColumn("textDescription", remove_special_chars_udf(col("textDescription")))

    stopwords = StopWordsRemover.loadDefaultStopWords("english")
    stopwords_remover = StopWordsRemover(inputCol="textDescription", outputCol="textDescription_wos", stopWords=stopwords)
    pLines = stopwords_remover.transform(pLines).drop("textDescription")

    pLines = pLines.withColumn("textDescription", concat_ws(" ", "textDescription_wos")).drop('textDescription_wos')

    return pLines

def explode_productLines_description(spark :pyspark.sql.SparkSession) -> pyspark.sql.DataFrame:
    ''' 
    some text
    '''
    pLines = load_data_from_csv(spark, "productlines.csv")
    pLines = pLines.withColumn("textDescription", split(col("textDescription"), " "))
    return pLines.select(pLines.productLine, explode(pLines.textDescription).alias('description'))


def get_customer_info(spark :pyspark.sql.SparkSession) -> pyspark.sql.DataFrame:
    '''
    some text
    '''
    customer_schema = StructType([ \
      StructField("custID",StringType(),True), \
      StructField("custName",StringType(),True), \
      StructField("country",StringType(),True), \
      StructField("#orders",IntegerType(),True), \
      StructField("totalMoneySpent", DoubleType(), True), \
      StructField("creditLimit", IntegerType(), True), \
    ])

    orders = load_data_from_csv(spark, "orders.csv")
    customers = load_data_from_csv(spark, "customers.csv")
    payments = load_data_from_flatfile(spark, "payments.txt")
    oc = customers.join(orders, 'customerNumber', 'left').drop(orders.customerNumber).groupBy('customerNumber').count()
    pc = customers.join(payments, 'customerNumber', 'left').drop(payments.customerNumber).groupBy('customerNumber').agg(sum('amount'))
    customer_details = pc.join(oc, 'customerNumber').join(customers, 'customerNumber').select('customerNumber', 'customerName', \
                       'country', 'count', col('sum(amount)').alias('totalAmount'), 'creditLimit')
    
    return customer_details.toDF(*customer_schema.fieldNames())

@udf
def categorize_products(buy_price):
    if buy_price < 40:
        return "Affordable"
    elif buy_price < 80:
        return "Moderate"
    else:
        return "Expensive"
    
def retrun_product_category(spark :pyspark.sql.SparkSession) -> pyspark.sql.DataFrame:
    ''' 
    '''
    products = load_data_from_csv(spark, "products.csv")
    products = products.withColumn('buyPrice', when(col('buyPrice').cast("integer").isNotNull(), \
                                col('buyPrice').cast("integer")).otherwise(0))
    return products.withColumn("productCategory", categorize_products(products["buyPrice"]))


def return_orders_by_day_by_status(spark :pyspark.sql.SparkSession) -> pyspark.sql.DataFrame:
    '''
    '''
    orders = load_data_from_csv(spark, "orders.csv")
    return orders.select(dayofweek('orderDate').alias('weekDay'), '*').groupBy('weekDay').pivot('status') \
    .agg(count('orderNumber').alias('totalOrders')).orderBy('weekDay')


# <------------------------------- ANALYTICS --------------------------------------------->
def return_top_5_employees(spark :pyspark.sql.SparkSession) -> pyspark.sql.DataFrame:
    '''
    Return the top 5 employees(Sales Representatives) who made maximum no. of
    sales throughout the given timeperiod. You can consider all sales with all statuses.
    
    Return a spark dataframe with following columns.
    employeeNumber|firstName|lastName |TotalOrders
    '''

    orders = load_data_from_csv(spark, "orders.csv")
    employees = load_data_from_csv(spark, "employees.csv")
    customers = load_data_from_csv(spark, "customers.csv")
    
    top_5_emp = orders.join(customers, 'customerNumber').groupBy('customerNumber').agg(count('orderNumber').alias('totalOrders'), \
                first('salesRepEmployeeNumber').alias('salesRepEmployeeNumber')).groupBy('salesRepEmployeeNumber') \
                .agg(sum('totalOrders').alias('totalOrders')).orderBy(col('totalOrders').desc()).limit(5) \
                .join(employees, col("salesRepEmployeeNumber")==employees.employeeNumber) \
                .select('firstName', 'lastName', 'totalOrders').orderBy(col('totalOrders').desc())
    return top_5_emp

def report_cancelled_orders(spark :pyspark.sql.SparkSession) -> pyspark.sql.DataFrame:
    '''
    Return email adresses of your manager to send a report about the cancelled orders. Return
    Order details and a column to define if the cancelled order has been shipped already
    or not('Yes' or  'No').
     
    Return a spark dataframe with following columns.
    orderNumber| isShipped| status| customerNumber| salesRepEmployeeNumber| reportsTo| email|
    '''
    
    load_data_from_csv(spark, "customers.csv").createOrReplaceTempView("customers_sql")
    load_data_from_csv(spark, "employees.csv").createOrReplaceTempView("employees_sql")
    load_data_from_csv(spark, "orders.csv").createOrReplaceTempView("orders_sql")


    emails = spark.sql(" select oce.*, email from employees_sql\
              join (select oc.*, reportsTo from employees_sql \
              join (select orders.*, salesRepEmployeeNumber from customers_sql \
              join (select orderNumber, case when shippedDate = 'NULL' then 'No' else 'Yes' end as isShipped, \
              comments, customerNumber from orders_sql where status='Cancelled') as orders \
              on customers_sql.customerNumber == orders.customerNumber) as oc \
              on employees_sql.employeeNumber == oc.salesRepEmployeeNumber) as oce \
              on employees_sql.employeeNumber == oce.reportsTo \
            ")
    return emails

def return_top_5_big_spenders(spark :pyspark.sql.SparkSession) -> pyspark.sql.DataFrame:
    '''
    Return top 5 big spenders who had spent the most $(highest order value).
    
    Return a spark dataframe with following columns.
    customerNumber| customerName| totalOrderValue|
    '''
    
    customers_df = load_data_from_csv(spark, "customers.csv")
    orders_df = load_data_from_csv(spark, "orders.csv")
    orderdetails_df = load_data_from_mysql(spark, "classicmodels", 'orderdetails')
    merged_data = customers_df.join(orders_df, "customerNumber").join(orderdetails_df, "orderNumber")
    # Calculate the total order value for each customer
    total_order_value = merged_data.groupBy("customerNumber", "customerName") \
    .agg(sum(merged_data.priceEach * merged_data.quantityOrdered).alias("totalOrderValue")) \
    .orderBy(col('totalOrderValue').desc()).limit(5)
    # print(total_order_value.show(truncate=False))
    # Return the total order value DataFrame
    return total_order_value

def return_top_5_big_spend_countries(spark :pyspark.sql.SparkSession) -> pyspark.sql.DataFrame:
    '''
    Return top 5 big countries which had spent the most $(highest order value).
    
    Return a spark dataframe with following columns.
    |country|totalOrderValue|
    '''
    
    customers_df = load_data_from_csv(spark, "customers.csv")
    orders_df = load_data_from_csv(spark, "orders.csv")
    orderdetails_df = load_data_from_mysql(spark, "classicmodels", 'orderdetails')
    merged_data = customers_df.join(orders_df, "customerNumber").join(orderdetails_df, "orderNumber")
    # Calculate the total order value for each customer
    total_order_value = merged_data.groupBy("country") \
    .agg(sum(merged_data.priceEach * merged_data.quantityOrdered).alias("totalOrderValue")) \
    .orderBy(col('totalOrderValue').desc()).limit(5)
    # print(total_order_value.show(truncate=False))
    # Return the total order value DataFrame
    return total_order_value
