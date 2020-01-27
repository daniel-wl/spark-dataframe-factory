using System;
using System.Collections.Generic;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

namespace Spark.DataframeFactory.Core
{
    public class DataframeFactory
    {
        internal SparkSession Spark { get; set; }

        public DataframeFactory(SparkSession spark)
        {
            if (spark == null)
            {
                throw new ArgumentNullException(nameof(spark));
            }

            Spark = spark;
        }

        public DataFrame Build()
        {
            return Build(
                new [] { new GenericRow(new object[] { }) },
                new Microsoft.Spark.Sql.Types.StructType(new StructField[] { }));
        }

        public DataFrame Build(IEnumerable<GenericRow> data, StructType schema)
        {
            return Spark.CreateDataFrame(data, schema);
        }
    }
}
