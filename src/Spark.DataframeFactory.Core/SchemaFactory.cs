using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.Sql.Types;

namespace Spark.DataframeFactory.Core
{
    public static class SchemaFactory
    {
        public static StructType Build(IEnumerable<Tuple<string, string>> columnDefinitions)
        {
            var schema = columnDefinitions.Select(definition =>
            {
                return new StructField(definition.Item1, ParseDataType(definition.Item2));
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
                    return new ByteType();
                case "binary":
                    return new BinaryType();
                case "short":
                    return new ShortType();
                case "integer":
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
                    throw new NotSupportedException($"Mapping type {stringType} is not supported.");
            }

        }
    }
}
