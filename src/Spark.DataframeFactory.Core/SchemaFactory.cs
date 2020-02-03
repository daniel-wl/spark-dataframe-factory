using System;
using System.Collections.Generic;
using System.Linq;
using Bogus;
using Microsoft.Spark.Sql.Types;

namespace Spark.DataframeFactory.Core
{
    public static class SchemaFactory
    {
        public static StructType Build(IEnumerable<Tuple<string, string>> columnDefinitions)
        {
            var schema = columnDefinitions.Select(definition =>
            {
                return new StructField(definition.Item1, ParseDataType(definition.Item2), new Faker().Random.Bool());
            });

            return new StructType(schema);
        }

        internal static DataType ParseDataType(string stringType)
        {
            switch (stringType)
            {
                case "boolean":
                    return new BooleanType();
                case "byte":
                    throw new NotSupportedException($"Spark .Net doesn't support type {stringType} yet.");
                case "binary":
                    throw new NotSupportedException($"Spark .Net doesn't support type {stringType} yet.");
                case "short":
                    return new ShortType();
                case "integer":
                    return new IntegerType();
                case "long":
                    return new LongType();
                case "float":
                    throw new NotSupportedException($"Spark .Net doesn't support type {stringType} yet.");
                case "double":
                    return new DoubleType();
                case "string":
                    return new StringType();
                case "date":
                    throw new NotSupportedException($"Spark .Net doesn't support type {stringType} yet.");
                case "timestamp":
                    throw new NotSupportedException($"Spark .Net doesn't support type {stringType} yet.");
                default:
                    throw new NotSupportedException($"Mapping type {stringType} is not supported.");
            }

        }
    }
}
