package kairosdb.export.csv.conf;


import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class CommandCli {

  private static final int MAX_HELP_CONSOLE_WIDTH = 88;
  private final String HELP_ARGS = "help";
  private final String CONFIG_ARGS = "cf";
  private final String CONFIG_NAME = "config file";

  public static void main(String[] args) {

  }

  private Options createOptions() {
    Options options = new Options();
    Option help = new Option(HELP_ARGS, false, "Display help information");
    help.setRequired(false);
    options.addOption(help);

    Option config = Option.builder(CONFIG_ARGS).argName(CONFIG_NAME).hasArg()
        .desc("Config file path (optional)").build();
    options.addOption(config);

    return options;
  }

  private boolean parseParams(CommandLine commandLine, CommandLineParser parser, Options options,
      String[] args, HelpFormatter hf) {
    try {
      commandLine = parser.parse(options, args);
      if (commandLine.hasOption(HELP_ARGS)) {
        hf.printHelp(Constants.CONSOLE_PREFIX, options, true);
        return false;
      }

      if (commandLine.hasOption(CONFIG_ARGS)) {
        System.setProperty(Constants.REST_CONF, commandLine.getOptionValue(CONFIG_ARGS));
      }

    } catch (ParseException e) {
      System.out.println("Require more params input, please check the following hint.");
      hf.printHelp(Constants.CONSOLE_PREFIX, options, true);
      return false;
    } catch (Exception e) {
      System.out.println("Error params input, because " + e.getMessage());
      hf.printHelp(Constants.CONSOLE_PREFIX, options, true);
      return false;
    }
    return true;
  }

  public boolean init(String[] args) {
    Options options = createOptions();
    HelpFormatter hf = new HelpFormatter();
    hf.setWidth(MAX_HELP_CONSOLE_WIDTH);
    CommandLine commandLine = null;
    CommandLineParser parser = new DefaultParser();

    if (args == null || args.length == 0) {
      System.out.println("Require more params input, please check the following hint.");
      hf.printHelp(Constants.CONSOLE_PREFIX, options, true);
      return false;
    }
    return parseParams(commandLine, parser, options, args, hf);
  }

}
