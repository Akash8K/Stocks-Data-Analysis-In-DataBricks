# Databricks notebook source
# MAGIC %md 
# MAGIC <h1 style="text-align:center"><font style="color:red; font-size:70px;">Stocks Data Analysis</font></h1>

# COMMAND ----------

 # Compare High And Low's with Respect to companies
 # Calculate Date Range of Data
 # Add A column called Day of Week
 # Is there any trend on the day of the week (trend = market fluctuation -gap betn open and close prices)
 # Standerd Deviation-mean and median of closing prices--> compare for all 5 companies
 # Identify the day when market fallen the most --> most down
 # Identify the day when market acted bullishly --> most up
 # Check yearly Volume   
 # check the relation betn close and adj close.

# COMMAND ----------

# MAGIC %md 
# MAGIC <h3 style="text-align:center"><font style="color:#02c0fa; font-size:50px;">DATABASE</font></h3>

# COMMAND ----------

# DBTITLE 1,Creating The DataBase 
# MAGIC %sql
# MAGIC create database PortFolio
# MAGIC 
# MAGIC --database needs to be created again if using community version of databricks

# COMMAND ----------

# MAGIC %md 
# MAGIC <h3 style="text-align:center"><font style="color:#02c0fa; font-size:50px;">TABLES</font></h3>

# COMMAND ----------

# DBTITLE 1,Create Tables On Top of CSV files uploaded inside HDFS
# MAGIC %sql
# MAGIC 
# MAGIC --table 1
# MAGIC CREATE TABLE PortFolio.AdaniPorts 
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   header "true",
# MAGIC   inferSchema "true"
# MAGIC )
# MAGIC LOCATION 'dbfs:/FileStore/Project/ADANIPORTS.csv';
# MAGIC 
# MAGIC --table 2
# MAGIC CREATE TABLE PortFolio.AsianPaint
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   header "true",
# MAGIC   inferSchema "true"
# MAGIC )
# MAGIC LOCATION 'dbfs:/FileStore/Project/ASIANPAINT.csv';
# MAGIC 
# MAGIC --table 3
# MAGIC CREATE TABLE PortFolio.HdfcBank 
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   header "true",
# MAGIC   inferSchema "true"
# MAGIC )
# MAGIC LOCATION 'dbfs:/FileStore/Project/HDFCBANK.csv';
# MAGIC 
# MAGIC --table 4
# MAGIC CREATE TABLE PortFolio.TataSteel 
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   header "true",
# MAGIC   inferSchema "true"
# MAGIC )
# MAGIC LOCATION 'dbfs:/FileStore/Project/TATASTEEL.csv';
# MAGIC 
# MAGIC --table 5
# MAGIC CREATE TABLE PortFolio.Tcs 
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   header "true",
# MAGIC   inferSchema "true"
# MAGIC )
# MAGIC LOCATION 'dbfs:/FileStore/Project/TCS.csv';

# COMMAND ----------

# MAGIC %md 
# MAGIC <h3 style="text-align:center"><font style="color:#02c0fa; font-size:50px;">HIGH AND LOW VALUES COMPARISON</font></h3>

# COMMAND ----------

# DBTITLE 1, fetching Max and Min Values of High & Low Columns company wise
# MAGIC %sql
# MAGIC select Tcs.* from 
# MAGIC (select Date, max(High) as Tcs_High, min(Low) as Tcs_Low from PortFolio.Tcs group By Date) Tcs

# COMMAND ----------

# MAGIC %sql
# MAGIC select AdaniPorts.* from 
# MAGIC (select Date, max(High) as AdaniPorts_High, min(Low) as AdaniPorts_Low from PortFolio.AdaniPorts group By Date) AdaniPorts

# COMMAND ----------

# MAGIC %sql
# MAGIC select AsianPaint.* from 
# MAGIC (select Date, max(High) as AsianPaint_High, min(Low) as AsianPaint_Low from PortFolio.AsianPaint group By Date) AsianPaint

# COMMAND ----------

# MAGIC %sql
# MAGIC select HdfcBank.* from 
# MAGIC (select Date, max(High) as HdfcBank_High, min(Low) as HdfcBank_Low from PortFolio.HdfcBank group By Date) HdfcBank

# COMMAND ----------

# MAGIC %sql
# MAGIC select TataSteel.* from 
# MAGIC (select Date, max(High) as TataSteel_High, min(Low) as TataSteel_Low from PortFolio.TataSteel group By Date) TataSteel

# COMMAND ----------

# MAGIC %md 
# MAGIC <h3 style="text-align:center"><font style="color:#02c0fa; font-size:50px;">NEW TABLES WITH UPDATED STRUCTURE</font></h3>

# COMMAND ----------

# DBTITLE 1,Create Tables with new columns to add data in them
# MAGIC %sql 
# MAGIC create table PortFolio.New_Tcs (
# MAGIC Date date,
# MAGIC Open double,
# MAGIC High double,
# MAGIC Low double,
# MAGIC Close double,
# MAGIC Adj_Close double,
# MAGIC Volume integer,
# MAGIC DayInWeek string
# MAGIC )

# COMMAND ----------

# MAGIC %sql 
# MAGIC create table PortFolio.New_HdfcBank (
# MAGIC Date date,
# MAGIC Open double,
# MAGIC High double,
# MAGIC Low double,
# MAGIC Close double,
# MAGIC Adj_Close double,
# MAGIC Volume integer,
# MAGIC DayInWeek string
# MAGIC )

# COMMAND ----------

# MAGIC %sql 
# MAGIC create table PortFolio.New_AdaniPorts (
# MAGIC Date date,
# MAGIC Open double,
# MAGIC High double,
# MAGIC Low double,
# MAGIC Close double,
# MAGIC Adj_Close double,
# MAGIC Volume integer,
# MAGIC DayInWeek string
# MAGIC )

# COMMAND ----------

# MAGIC %sql 
# MAGIC create table PortFolio.New_AsianPaint (
# MAGIC Date date,
# MAGIC Open double,
# MAGIC High double,
# MAGIC Low double,
# MAGIC Close double,
# MAGIC Adj_Close double,
# MAGIC Volume integer,
# MAGIC DayInWeek string
# MAGIC )

# COMMAND ----------

# MAGIC %sql 
# MAGIC create table PortFolio.New_TataSteel (
# MAGIC Date date,
# MAGIC Open double,
# MAGIC High double,
# MAGIC Low double,
# MAGIC Close double,
# MAGIC Adj_Close double,
# MAGIC Volume integer,
# MAGIC DayInWeek string
# MAGIC )

# COMMAND ----------

# DBTITLE 1,If table creation error - already exists then run this and drop table
# MAGIC %sql
# MAGIC SHOW TABLES IN PortFolio;

# COMMAND ----------

# DBTITLE 1,To remove tables if already exists

#drop table PortFolio.New_Tcs
#or use this python function to drop    
# dbutils.fs.rm('dbfs:/user/hive/warehouse/portfolio.db/new_tatasteel',recurse=True)

# COMMAND ----------

# MAGIC %md 
# MAGIC <h3 style="text-align:center"><font style="color:#02c0fa; font-size:50px;">DATA INSERTION</font></h3>

# COMMAND ----------

# DBTITLE 1,Now Insert the days inside the new table using old tables
# MAGIC %sql
# MAGIC 
# MAGIC --Table 1 insertion (Insert Only Once After table creation)
# MAGIC 
# MAGIC insert into PortFolio.New_Tcs
# MAGIC select *,
# MAGIC case
# MAGIC when dayofweek(Date)=1 then 'sunday'
# MAGIC when dayofweek(Date)=2 then 'monday'
# MAGIC when dayofweek(Date)=3 then 'Tuesday'
# MAGIC when dayofweek(Date)=4 then 'wedensday'
# MAGIC when dayofweek(Date)=5 then 'Thursday'
# MAGIC when dayofweek(Date)=6 then 'Friday'
# MAGIC when dayofweek(Date)=7 then 'Saturday'
# MAGIC end as DayInWeek
# MAGIC 
# MAGIC from PortFolio.Tcs;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --Table 2 insertion (Insert Only Once After table creation)
# MAGIC 
# MAGIC insert into PortFolio.New_AsianPaint
# MAGIC select *,
# MAGIC case
# MAGIC when dayofweek(Date)=1 then 'sunday'
# MAGIC when dayofweek(Date)=2 then 'monday'
# MAGIC when dayofweek(Date)=3 then 'Tuesday'
# MAGIC when dayofweek(Date)=4 then 'wedensday'
# MAGIC when dayofweek(Date)=5 then 'Thursday'
# MAGIC when dayofweek(Date)=6 then 'Friday'
# MAGIC when dayofweek(Date)=7 then 'Saturday'
# MAGIC end as DayInWeek
# MAGIC 
# MAGIC from PortFolio.AsianPaint;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --Table 3 insertion (Insert Only Once After table creation)
# MAGIC 
# MAGIC insert into PortFolio.New_AdaniPorts
# MAGIC select *,
# MAGIC case
# MAGIC when dayofweek(Date)=1 then 'sunday'
# MAGIC when dayofweek(Date)=2 then 'monday'
# MAGIC when dayofweek(Date)=3 then 'Tuesday'
# MAGIC when dayofweek(Date)=4 then 'wedensday'
# MAGIC when dayofweek(Date)=5 then 'Thursday'
# MAGIC when dayofweek(Date)=6 then 'Friday'
# MAGIC when dayofweek(Date)=7 then 'Saturday'
# MAGIC end as DayInWeek
# MAGIC 
# MAGIC from PortFolio.AdaniPorts;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --Table 4 insertion (Insert Only Once After table creation)
# MAGIC 
# MAGIC insert into PortFolio.New_HdfcBank
# MAGIC select *,
# MAGIC case
# MAGIC when dayofweek(Date)=1 then 'sunday'
# MAGIC when dayofweek(Date)=2 then 'monday'
# MAGIC when dayofweek(Date)=3 then 'Tuesday'
# MAGIC when dayofweek(Date)=4 then 'wedensday'
# MAGIC when dayofweek(Date)=5 then 'Thursday'
# MAGIC when dayofweek(Date)=6 then 'Friday'
# MAGIC when dayofweek(Date)=7 then 'Saturday'
# MAGIC end as DayInWeek
# MAGIC 
# MAGIC from PortFolio.HdfcBank;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --Table 5 insertion (Insert Only Once After table creation)
# MAGIC 
# MAGIC insert into PortFolio.New_TataSteel
# MAGIC select *,
# MAGIC case
# MAGIC when dayofweek(Date)=1 then 'sunday'
# MAGIC when dayofweek(Date)=2 then 'monday'
# MAGIC when dayofweek(Date)=3 then 'Tuesday'
# MAGIC when dayofweek(Date)=4 then 'wedensday'
# MAGIC when dayofweek(Date)=5 then 'Thursday'
# MAGIC when dayofweek(Date)=6 then 'Friday'
# MAGIC when dayofweek(Date)=7 then 'Saturday'
# MAGIC end as DayInWeek
# MAGIC 
# MAGIC from PortFolio.TataSteel;

# COMMAND ----------

# MAGIC %md 
# MAGIC <h3 style="text-align:center"><font style="color:#02c0fa; font-size:50px;">TREND AND FLUCTUATION</font></h3>

# COMMAND ----------

# DBTITLE 1,Is there any trend on the day of the week (trend = market fluctuation -gap betn open and close prices)
# MAGIC %sql 
# MAGIC -->substracting open column values from close column to check market fluctuation to corresponding Day in week
# MAGIC 
# MAGIC --> company 1
# MAGIC select (Open-Close) as Fluctuation,DayInWeek from PortFolio.New_Tcs

# COMMAND ----------

# MAGIC %sql 
# MAGIC -->substracting open column values from close column to check market fluctuation to corresponding Day in week
# MAGIC 
# MAGIC --> company 2
# MAGIC select (Open-Close) as Fluctuation,DayInWeek from PortFolio.New_AdaniPorts

# COMMAND ----------

# MAGIC %sql 
# MAGIC -->substracting open column values from close column to check market fluctuation to corresponding Day in week
# MAGIC 
# MAGIC --> company 3
# MAGIC select (Open-Close) as Fluctuation,DayInWeek from PortFolio.New_AsianPaint

# COMMAND ----------

# MAGIC %sql 
# MAGIC -->substracting open column values from close column to check market fluctuation to corresponding Day in week
# MAGIC 
# MAGIC --> company 4
# MAGIC select (Open-Close) as Fluctuation,DayInWeek from PortFolio.New_HdfcBank

# COMMAND ----------

# MAGIC %sql 
# MAGIC -->substracting open column values from close column to check market fluctuation to corresponding Day in week
# MAGIC 
# MAGIC --> company 5
# MAGIC select (Open-Close) as Fluctuation,DayInWeek from PortFolio.New_TataSteel

# COMMAND ----------

# MAGIC %md 
# MAGIC <h3 style="text-align:center"><font style="color:#02c0fa; font-size:50px;">AVERAGE DAILY VOLUME</font></h3>

# COMMAND ----------

# DBTITLE 1,Checking Highest Volume Year-Wise or Day-Wise
# MAGIC %sql
# MAGIC select DayInWeek,Date,Volume from PortFolio.New_Tcs

# COMMAND ----------

# MAGIC %sql
# MAGIC select DayInWeek,Date,Volume from PortFolio.New_AdaniPorts

# COMMAND ----------

# MAGIC %sql
# MAGIC select DayInWeek,Date,Volume from PortFolio.New_AsianPaint

# COMMAND ----------

# MAGIC %sql
# MAGIC select DayInWeek,Date,Volume from PortFolio.New_HdfcBank

# COMMAND ----------

# MAGIC %sql
# MAGIC select DayInWeek,Date,Volume from PortFolio.New_TataSteel

# COMMAND ----------

# MAGIC %md 
# MAGIC <h3 style="text-align:center"><font style="color:#02c0fa; font-size:50px;">RELATION BETWEEN CLOSE AND ADJUSTED CLOSE VALUES</font></h3>

# COMMAND ----------

# DBTITLE 1,Finding the Relation Between Close and Adj Close
# MAGIC %sql
# MAGIC -->Company 1
# MAGIC 
# MAGIC select Close,Adj_Close,Date from PortFolio.New_Tcs

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -->Company 2
# MAGIC select Close,Adj_Close,Date from PortFolio.New_AdaniPorts

# COMMAND ----------

# MAGIC %sql
# MAGIC -->Company 3
# MAGIC select Close,Adj_Close,Date from PortFolio.New_AsianPaint

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -->Company 4
# MAGIC select Close,Adj_Close,Date from PortFolio.New_HdfcBank

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -->Company 5
# MAGIC select Close,Adj_Close,Date from PortFolio.New_TataSteel

# COMMAND ----------

# MAGIC %md 
# MAGIC <h3 style="text-align:center"><font style="color:#02c0fa; font-size:50px; text-transform: uppercase;">DAY WHEN MARKET ACTED BEARISHLY</font></h3>

# COMMAND ----------

# DBTITLE 1,Identify the day when market has fallen most
# MAGIC %sql
# MAGIC 
# MAGIC -->Company 1
# MAGIC select DayInWeek,Date,(Open-Close) as Most_Fallen from PortFolio.New_Tcs 
# MAGIC where (Open-Close)= (select min(Open-Close) from PortFolio.New_Tcs )

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -->Company 2
# MAGIC select DayInWeek,Date,(Open-Close) as Most_Fallen from PortFolio.New_AdaniPorts 
# MAGIC where (Open-Close)= (select min(Open-Close) from PortFolio.New_AdaniPorts )

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -->Company 3
# MAGIC select DayInWeek,Date,(Open-Close) as Most_Fallen from PortFolio.New_AsianPaint 
# MAGIC where (Open-Close)= (select min(Open-Close) from PortFolio.New_AsianPaint )

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -->Company 4
# MAGIC select DayInWeek,Date,(Open-Close) as Most_Fallen from PortFolio.New_HdfcBank 
# MAGIC where (Open-Close)= (select min(Open-Close) from PortFolio.New_HdfcBank )

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -->Company 5
# MAGIC select DayInWeek,Date,(Open-Close) as Most_Fallen from PortFolio.New_TataSteel 
# MAGIC where (Open-Close)= (select min(Open-Close) from PortFolio.New_TataSteel )

# COMMAND ----------

# MAGIC %md 
# MAGIC <h3 style="text-align:center"><font style="color:#02c0fa; font-size:50px; text-transform: uppercase;">DAY WHEN MARKET ACTED BULLISHLY</font></h3>

# COMMAND ----------

# DBTITLE 1,Identify the day when Market Acted Bullishly- most Up
# MAGIC %sql
# MAGIC 
# MAGIC -->Company 1
# MAGIC select DayInWeek,Date,(Open-Close) as Acted_Bullishly from PortFolio.New_Tcs 
# MAGIC where (Open-Close)= (select max(Open-Close) from PortFolio.New_Tcs )

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -->Company 2
# MAGIC select DayInWeek,Date,(Open-Close) as Acted_Bullishly from PortFolio.New_AdaniPorts
# MAGIC where (Open-Close)= (select max(Open-Close) from PortFolio.New_AdaniPorts )

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -->Company 3
# MAGIC select DayInWeek,Date,(Open-Close) as Acted_Bullishly from PortFolio.New_AsianPaint 
# MAGIC where (Open-Close)= (select max(Open-Close) from PortFolio.New_AsianPaint )

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -->Company 4
# MAGIC select DayInWeek,Date,(Open-Close) as Acted_Bullishly from PortFolio.New_HdfcBank
# MAGIC where (Open-Close)= (select max(Open-Close) from PortFolio.New_HdfcBank )

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -->Company 5
# MAGIC select DayInWeek,Date,(Open-Close) as Acted_Bullishly from PortFolio.New_TataSteel 
# MAGIC where (Open-Close)= (select max(Open-Close) from PortFolio.New_TataSteel )

# COMMAND ----------

# MAGIC %md 
# MAGIC <h3 style="text-align:center"><font style="color:#02c0fa; font-size:50px; text-transform: uppercase;">YEARLY VOLUME VISUALIZATION</font></h3>

# COMMAND ----------

# DBTITLE 1,Find/Check Yearly Volume Graph
# MAGIC %sql
# MAGIC 
# MAGIC -->Company 1
# MAGIC select Date,Volume from PortFolio.New_Tcs

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -->Company 2
# MAGIC select Date,Volume from PortFolio.New_AdaniPorts

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -->Company 3
# MAGIC select Date,Volume from PortFolio.New_AsianPaint

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -->Company 4
# MAGIC select Date,Volume from PortFolio.New_HdfcBank

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -->Company 5
# MAGIC select Date,Volume from PortFolio.New_TataSteel
