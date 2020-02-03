using System;
using System.Linq;
using CommandLine;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Spark.DataframeFactory.Core;

namespace Spark.DataframeFactory.Console
{
    public static class Program
    {
        public static void Main(string[] args)
        {
            var parser = new Parser();
            parser.ParseArguments<Options>(args)
                .WithParsed(options => Run(options))
                .WithNotParsed(error => System.Console.WriteLine(error));
            System.Console.ReadLine();
        }

        public static void Run(Options options)
        {
            SparkSession spark = SparkSession.Builder().GetOrCreate();
            var df = new Core.DataframeFactory(spark, ParseSchema(options)).Build(options.Rows);
            df.Write()
                .Format(options.OutputFormat.ToString().ToLower())
                .Save(options.OutputPath);
        }

        public static StructType ParseSchema(Options options)
        {
            var columnDefinitions = options.Columns.Split(',');
            if (!columnDefinitions.Any())
            {
                throw new InvalidOperationException($"Unable to get column definitions from column string {options.Columns}.");
            }

            return SchemaFactory.Build(columnDefinitions.Select(definitionString =>
            {
                var definition = definitionString.Split(':');
                if (definition.Length != 2)
                {
                    throw new InvalidOperationException($"Unable to get column definition from {definition}.");
                }
                return new Tuple<string, string>(definition[0], definition[1]);
            }));
        }
    }
}
