using System;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

namespace Spark.DataframeFactory.Core
{
    public class DataframeFactory
    {
        internal SparkSession Spark { get; set; }
        internal StructType Schema { get; set; }

        public DataframeFactory(SparkSession spark, StructType schema)
        {
            if (spark == null)
            {
                throw new ArgumentNullException(nameof(spark));
            }

            if (schema == null)
            {
                throw new ArgumentNullException(nameof(schema));
            }

            Spark = spark;
            Schema = schema;
        }

        public DataFrame Build(int rows)
        {
            return Spark.CreateDataFrame(RowFactory.Build(rows, Schema), Schema);
        }
    }
}
