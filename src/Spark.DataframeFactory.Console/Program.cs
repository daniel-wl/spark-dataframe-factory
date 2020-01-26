using System;
using Spark.DataframeFactory.Core;

namespace Spark.DataframeFactory.Console {
    class Program {
        static void Main (string[] args) {
            SparkTest.RunSpark();
            System.Console.ReadLine();
        }
    }
}