using System;
using System.Collections.Generic;
using System.Linq;
using Bogus;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

namespace Spark.DataframeFactory.Core
{
    public static class RowFactory
    {
        internal static Faker Faker = new Faker();

        public static IEnumerable<GenericRow> Build(int rows, StructType schema)
        {
            for (var i = 0; i < rows; i++)
            {
                yield return CreateRow(schema);
            }
        }

        internal static GenericRow CreateRow(StructType schema)
        {
            return new GenericRow(schema.Fields.Select(field => GetValue(field)).ToArray());
        }

        internal static object GetValue(StructField field)
        {
            if (field.IsNullable && Faker.Random.Bool())
            {
                return null;
            }

            var dataType = field.DataType;
            if (dataType is BooleanType)
            {
                return Faker.Random.Bool();
            }
            if (dataType is ByteType)
            {
                return Faker.Random.Byte();
            }
            if (dataType is BinaryType)
            {
                return Faker.Random.Bytes(Faker.Random.Int(0, 100));
            }
            if (dataType is ShortType)
            {
                return Faker.Random.Short();
            }
            if (dataType is IntegerType)
            {
                return Faker.Random.Int();
            }
            if (dataType is LongType)
            {
                return Faker.Random.Long();
            }
            if (dataType is FloatType)
            {
                return Faker.Random.Float();
            }
            if (dataType is DoubleType)
            {
                return Faker.Random.Double();
            }
            if (dataType is StringType)
            {
                return Faker.Random.String2(Faker.Random.Int(0, 100));
            }
            if (dataType is DateType || dataType is TimestampType)
            {
                return Faker.Date.Recent();
            }

            throw new NotSupportedException($"Creating data for type {dataType.GetType().FullName} is not supported.");
        }
    }
}
