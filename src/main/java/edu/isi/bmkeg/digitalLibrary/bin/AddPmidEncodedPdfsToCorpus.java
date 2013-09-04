package edu.isi.bmkeg.digitalLibrary.bin;

import java.io.File;
import java.util.Map;

import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.isi.bmkeg.digitalLibrary.controller.DigitalLibraryEngine;

public class AddPmidEncodedPdfsToCorpus {

	private static Logger logger = Logger.getLogger(AddPmidEncodedPdfsToCorpus.class);

	public static class Options {

		@Option(name = "-pdfs", usage = "Pdfs directory o file", required = true, metaVar = "PDF-DIR-OR-FILE")
		public File pdfFileOrDr;
		
		@Option(name = "-corpus", usage = "Corpus name", required = true, metaVar = "CORPUS")
		public String corpusName;
		
		@Option(name = "-rules", usage = "Rules file", required = false, metaVar = "FILE")
		public File pdfRuleFile = null;
		
		@Option(name = "-l", usage = "Database login", required = true, metaVar = "LOGIN")
		public String login = "";

		@Option(name = "-p", usage = "Database password", required = true, metaVar = "PASSWD")
		public String password = "";

		@Option(name = "-db", usage = "Database name", required = true, metaVar  = "DBNAME")
		public String dbName = "";
		
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		Options options = new Options();
		
		CmdLineParser parser = new CmdLineParser(options);

		try {
			
			parser.parseArgument(args);

			if( !options.pdfFileOrDr.exists() ) {
				throw new CmdLineException(parser, options.pdfFileOrDr.getAbsolutePath() + " does not exist.");
			}
		
			DigitalLibraryEngine de = null;
			
			if (options.pdfRuleFile != null) {
				logger.info("Using rulefile " + options.pdfRuleFile.getAbsolutePath());
				de = new DigitalLibraryEngine(options.pdfRuleFile);
			} else {
				de = new DigitalLibraryEngine();
			}		
			de.initializeVpdmfDao(options.login, options.password, options.dbName);
			
			Map<Integer,Long> mapPmidsToVpdmfids = de.insertPmidPdfFileOrDir(options.pdfFileOrDr);
			
			de.loadArticlesFromPmidListToCorpus(mapPmidsToVpdmfids.keySet(), options.corpusName);		

		} catch (CmdLineException e) {

			System.err.println(e.getMessage());
			System.err.print("Arguments: ");
			parser.printSingleLineUsage(System.err);
			System.err.println("\n\n Options: \n");
			parser.printUsage(System.err);
			System.exit(-1);
		
		}

	}

}
