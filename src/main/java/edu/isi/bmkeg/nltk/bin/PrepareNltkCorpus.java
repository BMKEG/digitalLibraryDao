package edu.isi.bmkeg.nltk.bin;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.isi.bmkeg.digitalLibrary.controller.DigitalLibraryEngine;

public class PrepareNltkCorpus {

	private static Logger logger = Logger.getLogger(PrepareNltkCorpus.class);

	public static class Options {
		
		@Option(name = "-corpus", usage = "Corpus name", required = true, metaVar = "CORPUS")
		public String corpusName;
		
		@Option(name = "-l", usage = "Database login", required = true, metaVar = "LOGIN")
		public String login = "";

		@Option(name = "-p", usage = "Database password", required = true, metaVar = "PASSWD")
		public String password = "";

		@Option(name = "-db", usage = "Database name", required = true, metaVar  = "DBNAME")
		public String dbName = "";

		@Option(name = "-wd", usage = "Working directory", required = true, metaVar  = "WDIR")
		public String workingDirectory = "";
		
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		Options options = new Options();
		
		CmdLineParser parser = new CmdLineParser(options);

		DigitalLibraryEngine de = null;

		try {
			
			parser.parseArgument(args);
			
			de = new DigitalLibraryEngine();
			de.initializeVpdmfDao(options.login, options.password, options.dbName, options.workingDirectory);
			de.getDigLibDao().getCoreDao().connectToDb();

			String fileNames = de.getExtDigLibDao().listFileNamesInCorpus(options.corpusName);
			
			File outputDir = new File(options.workingDirectory + "/nltk/corpora/");
			if(outputDir.exists()) {
				outputDir.mkdirs();
			}
			File outputFile = new File(outputDir.getPath() + "/" + options.corpusName + "_fileList.txt");
			
			FileUtils.writeStringToFile(outputFile, fileNames);
			
		} catch (CmdLineException e) {

			System.err.println(e.getMessage());
			System.err.print("Arguments: ");
			parser.printSingleLineUsage(System.err);
			System.err.println("\n\n Options: \n");
			parser.printUsage(System.err);
			System.exit(-1);
		
		} finally {
			
			de.getDigLibDao().getCoreDao().closeDbConnection();
			
		}

	}

}
