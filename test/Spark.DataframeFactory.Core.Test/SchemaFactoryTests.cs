using System;
using System.Linq;
using Bogus;
using FluentAssertions;
using Microsoft.Spark.Sql.Types;
using Xunit;

namespace Spark.DataframeFactory.Core.Test
{
    public class SchemaFactoryTest
    {
        public static Faker Faker = new Faker();

        public class Build
        {
            [Fact]
            public void ReturnsStructType()
            {
                SchemaFactory.Build(new []
                {
                    new Tuple<string, string>("foo", "string")
                }).Should().BeOfType<StructType>();
            }

            [Fact]
            public void ReturnsCorrectNumberOfColumns()
            {
                var columnDefinitions = new int[Faker.Random.Int(1, 100)]
                    .Select(i => new Tuple<string, string>("foo", "string"));
                SchemaFactory.Build(columnDefinitions).Fields.Should().HaveCount(columnDefinitions.Count());
            }

            [Fact]
            public void ReturnsCorrectColumnDefinitions()
            {
                var dataTypes = Utilities.DataTypes;
                var columnDefinitions = new int[Faker.Random.Int(1, 100)]
                    .Select(i => new Tuple<string, string>(Faker.Random.Word(), dataTypes[Faker.Random.Int(0, dataTypes.Length - 1)]))
                    .ToList();
                var schema = SchemaFactory.Build(columnDefinitions);
                schema.Fields.Should().OnlyContain(field => columnDefinitions.Contains(new Tuple<string, string>(field.Name, field.DataType.SimpleString)));
            }
        }

        public class ParseDataType
        {
            [Fact]
            public void ReturnsCorrectDataType()
            {
                var dataTypes = Utilities.DataTypes;
                var toParse = dataTypes[Faker.Random.Int(0, dataTypes.Length - 1)];
                SchemaFactory.ParseDataType(toParse).SimpleString.Should().BeEquivalentTo(toParse);
            }

            [Fact]
            public void ThrowsOnUnknownDataType()
            {
                Action parse = () => SchemaFactory.ParseDataType(Faker.Random.Word());
                parse.Should().ThrowExactly<NotSupportedException>();
            }
        }
    }
}
