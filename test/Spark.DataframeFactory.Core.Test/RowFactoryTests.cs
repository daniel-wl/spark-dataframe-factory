using System;
using System.Linq;
using Bogus;
using FluentAssertions;
using Microsoft.Spark.Sql;
using Xunit;

namespace Spark.DataframeFactory.Core.Test
{
    public class RowFactoryTests
    {
        public static Faker Faker = new Faker();

        public class Build
        {
            [Fact]
            public void ReturnsCorrectNumberOfRows()
            {
                var count = Faker.Random.Int(1, 10000);
                RowFactory.Build(count, SchemaFactory.Build(new [] { new Tuple<string, string>("foo", "boolean") }))
                    .Should().HaveCount(count);
            }
        }

        public class CreateRow
        {
            [Fact]
            public void ReturnsRowWithCorrectSchema()
            {
                var dataTypes = Utilities.DataTypes;
                var schema = SchemaFactory.Build(new int[Faker.Random.Int(1, 100)]
                    .Select(i => new Tuple<string, string>(Faker.Random.Word(), dataTypes[Faker.Random.Int(0, dataTypes.Length - 1)])));

                var row = RowFactory.CreateRow(schema);
                row.Should().BeOfType<GenericRow>();
                row.Values.Should().OnlyContain(field => field == null ? true : field.GetType().Name == Utilities.GetTypeName(field));
            }
        }
    }
}
