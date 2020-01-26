# Spark Dataframe Factory
This project is intended as an exercise in creating spark jobs from .NET with the eventual goal of producing a unit testing library for creating arbitrary dataframes in memory.

## Prerequisites
All prerequisites of the [.Net Spark Project](https://github.com/dotnet/spark) apply here.

## Running
From the project root, run
```bash
spark-submit \
--class org.apache.spark.deploy.dotnet.DotnetRunner \
--master local ./src/Spark.DataframeFactory.Console/bin/Debug/netcoreapp2.0/microsoft-spark-2.4.x-0.8.0.jar dotnet ./src/Spark.DataframeFactory.Console/bin/Debug/netcoreapp2.0/Spark.DataframeFactory.Console.dll
```
