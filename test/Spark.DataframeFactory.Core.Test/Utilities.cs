using System;
using Microsoft.Spark.Sql.Types;

namespace Spark.DataframeFactory.Core.Test
{
    public static class Utilities
    {
        public static string[] DataTypes =>
            new string[]
            {
                new BooleanType().SimpleString,
                new ByteType().SimpleString,
                new BinaryType().SimpleString,
                new ShortType().SimpleString,
                new IntegerType().SimpleString,
                new LongType().SimpleString,
                new FloatType().SimpleString,
                new DoubleType().SimpleString,
                new StringType().SimpleString,
                new DateType().SimpleString,
                new TimestampType().SimpleString
            };

        public static string GetTypeName(object value)
        {
            if (value is bool)
            {
                return typeof(bool).Name;
            }
            if (value is byte)
            {
                return typeof(byte).Name;
            }
            if (value is byte[])
            {
                return typeof(byte[]).Name;
            }
            if (value is short)
            {
                return typeof(short).Name;
            }
            if (value is int)
            {
                return typeof(int).Name;
            }
            if (value is long)
            {
                return typeof(long).Name;
            }
            if (value is float)
            {
                return typeof(float).Name;
            }
            if (value is double)
            {
                return typeof(double).Name;
            }
            if (value is string)
            {
                return typeof(string).Name;
            }
            if (value is DateTime)
            {
                return typeof(DateTime).Name;
            }

            throw new InvalidOperationException($"Unknown type {value.GetType().FullName}");
        }
    }
}
