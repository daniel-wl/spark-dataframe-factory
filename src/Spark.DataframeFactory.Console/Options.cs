using CommandLine;

public class Options
{
    [Option('r', "rows", Required = false, Default = 10, HelpText = "The number of rows of data to generate.")]
    public int Rows { get; set; }

    [Option('c', "columns", Required = true, HelpText = "A comma-separated list of the dataframe columns in name:type format.")]
    public string Columns { get; set; }
}
