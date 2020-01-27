using CommandLine;

public class Options
{
    [Option('r', "rows", Required = false, Default = 10)]
    public int Rows { get; set; }
}
