using System;
using System.Collections.Generic;
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
            var factory = new Core.DataframeFactory(spark, ParseSchema(options));
            var df = factory.Build(options.Rows);
            df.Show();
        }

        public static StructType ParseSchema(Options options)
        {
            var columnDefinitions = options.Columns.Split(',');
            if (!columnDefinitions.Any())
            {
                throw new InvalidOperationException($"Unable to get column definitions from column string {options.Columns}.");
            }

            var schema = columnDefinitions.Select(definition =>
            {
                var column = definition.Split(':');
                if (!column.Any())
                {
                    throw new InvalidOperationException($"Unable to get column definition from {definition}.");
                }

                return new StructField(column[0], ParseDataType(column[1]));
            });

            return new StructType(schema);
        }

        public static DataType ParseDataType(string stringType)
        {
            switch (stringType)
            {
                case "bool":
                    return new BooleanType();
                case "byte":
                    return new ByteType();
                case "binary":
                    return new BinaryType();
                case "short":
                    return new ShortType();
                case "int":
                    return new IntegerType();
                case "long":
                    return new LongType();
                case "float":
                    return new FloatType();
                case "double":
                    return new DoubleType();
                case "string":
                    return new StringType();
                case "date":
                    return new DateType();
                case "timestamp":
                    return new TimestampType();
                default:
                    throw new NotImplementedException($"Mapping type {stringType} is not currently implemented.");
            }
        }
    }
}
