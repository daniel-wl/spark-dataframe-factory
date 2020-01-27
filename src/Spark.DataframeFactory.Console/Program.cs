using System;
using Microsoft.Spark.Sql;
using Spark.DataframeFactory.Core;

namespace Spark.DataframeFactory.Console
{
    class Program
    {
        static void Main(string[] args)
        {
            SparkSession spark = SparkSession.Builder().GetOrCreate();
            var factory = new Core.DataframeFactory(spark);
            factory.Build();
            System.Console.ReadLine();
        }
    }
}
