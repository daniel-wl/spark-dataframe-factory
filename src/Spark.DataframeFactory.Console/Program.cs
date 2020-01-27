using System;
using CommandLine;
using Microsoft.Spark.Sql;
using Spark.DataframeFactory.Core;

namespace Spark.DataframeFactory.Console
{
    public static class Program
    {
        public static void Main(string[] args)
        {
            var parser = new Parser();
            var result = parser.ParseArguments<Options>(args)
                .WithParsed(options => Run(options));
        }

        public static int Run(Options options)
        {
            SparkSession spark = SparkSession.Builder().GetOrCreate();
            var factory = new Core.DataframeFactory(spark);
            factory.Build();
            System.Console.ReadLine();
            return 0;
        }
    }
}
