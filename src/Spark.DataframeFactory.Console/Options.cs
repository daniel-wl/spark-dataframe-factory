using CommandLine;

namespace Spark.DataframeFactory.Console
{
    public class Options
    {
        [Option('r', "rows", Required = false, Default = 10, HelpText = "The number of rows of data to generate.")]
        public int Rows { get; set; }

        [Option('c', "columns", Required = true, HelpText = "A comma-separated list of the dataframe columns in name:type format.")]
        public string Columns { get; set; }

        [Option('o', "output-path", Required = true, HelpText = "The path to write the generated dataframe to.")]
        public string OutputPath { get; set; }

        [Option('f', "output-format", Required = false, Default = OutputFormat.Csv, HelpText = "The file format to write to.")]
        public OutputFormat OutputFormat { get; set; }
    }
}
