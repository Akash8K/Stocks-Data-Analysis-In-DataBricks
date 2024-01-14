# Stocks Data Analysis in Databricks

This Databricks notebook performs an in-depth analysis of stocks data for five companies (TCS, AdaniPorts, AsianPaint, HdfcBank, TataSteel). The analysis covers various aspects, including:

## 1. Database and Tables Creation
   - A database named "PortFolio" is created.
   - Tables are created for each company (AdaniPorts, AsianPaint, HdfcBank, TataSteel, TCS) based on CSV files uploaded into HDFS.

## 2. High and Low Values Comparison
   - Queries are executed to fetch the maximum and minimum values of High and Low columns for each company on specific dates.

## 3. New Tables with Updated Structure
   - New tables are created with additional columns (DayInWeek) to store data in a structured manner.

## 4. Data Insertion
   - Data from the original tables is inserted into the new tables, with an additional column indicating the day of the week.

## 5. Trend and Fluctuation
   - Market fluctuation is analyzed by subtracting open values from close values for each company.

## 6. Average Daily Volume
   - Daily volumes are displayed for each company, helping to identify trends and patterns.

## 7. Relation Between Close and Adjusted Close Values
   - The relationship between the Close and Adjusted Close values is examined for each company.

## 8. Day When Market Acted Bearishly
   - The day when the market has fallen the most is identified for each company.

## 9. Day When Market Acted Bullishly
   - The day when the market acted most bullishly (upward movement) is identified for each company.

## 10. Yearly Volume Visualization
    - Yearly volume data is visualized for each company.

Feel free to explore and adapt this notebook for your own analysis or use case.
