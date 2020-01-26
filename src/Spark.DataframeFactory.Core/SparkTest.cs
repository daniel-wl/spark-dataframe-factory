using System;
using Microsoft.Spark.Sql;

namespace Spark.DataframeFactory.Core {
    public static class SparkTest {
        public static void RunSpark () {
            var spark = SparkSession.Builder().GetOrCreate();
            Console.WriteLine ("#######");
            Console.WriteLine ($"Spark: {spark.ToString()}");
            Console.WriteLine ("#######");
        }
    }
}