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
            parser.ParseArguments<Options>(args)
                .WithParsed(options => Run(options));
            System.Console.ReadLine();
        }

        public static void Run(Options options)
        {
            SparkSession spark = SparkSession.Builder().GetOrCreate();
            var factory = new Core.DataframeFactory(spark, options.Rows);
            factory.Build();
        }
    }
}
