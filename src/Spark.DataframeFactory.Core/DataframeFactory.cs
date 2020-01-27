using System;
using System.Collections.Generic;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

namespace Spark.DataframeFactory.Core
{
    public class DataframeFactory
    {
        internal SparkSession Spark { get; set; }
        internal int Rows { get; set; }

        public DataframeFactory(SparkSession spark, int rows)
        {
            if (spark == null)
            {
                throw new ArgumentNullException(nameof(spark));
            }

            Spark = spark;
            Rows = rows;
        }

        public DataFrame Build()
        {
            return Build(
                new GenericRow[Rows],
                new Microsoft.Spark.Sql.Types.StructType(new StructField[] { }));
        }

        public DataFrame Build(IEnumerable<GenericRow> data, StructType schema)
        {
            return Spark.CreateDataFrame(data, schema);
        }
    }
}
