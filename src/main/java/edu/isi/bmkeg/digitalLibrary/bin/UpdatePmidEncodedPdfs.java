package edu.isi.bmkeg.digitalLibrary.bin;

import java.io.File;

import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.isi.bmkeg.digitalLibrary.bin.AddPmidEncodedPdfsToCorpus.Options;
import edu.isi.bmkeg.digitalLibrary.controller.DigitalLibraryEngine;
import edu.isi.bmkeg.vpdmf.model.definitions.VPDMf;

public class UpdatePmidEncodedPdfs {

	public static String USAGE = "arguments: <pdf-dir-or-file> <force-blocking:0/1> " +
			"<dbName> <login> <password> <workingDirectory> [<rule-file>]"; 

	public static class Options {

		@Option(name = "-pdfs", usage = "Pdfs directory o file", required = true, metaVar = "PDF-DIR-OR-FILE")
		public File pdfFileOrDir;
		
		@Option(name = "-forceBlocking", usage = "Force reblocking of PDF?", required = false, metaVar = "REBLOCK")
		public boolean forceBlocking = false;
		
		@Option(name = "-rules", usage = "Rules file", required = false, metaVar = "FILE")
		public File pdfRuleFile = null;
		
		@Option(name = "-l", usage = "Database login", required = true, metaVar = "LOGIN")
		public String login = "";

		@Option(name = "-p", usage = "Database password", required = true, metaVar = "PASSWD")
		public String password = "";

		@Option(name = "-db", usage = "Database name", required = true, metaVar  = "DBNAME")
		public String dbName = "";

		@Option(name = "-wd", usage = "Working directory", required = true, metaVar  = "WDIR")
		public String workingDirectory = "";
		
	}
	
	private static Logger logger = Logger.getLogger(UpdatePmidEncodedPdfs.class);

	private VPDMf top;
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		Options options = new Options();
		
		CmdLineParser parser = new CmdLineParser(options);

		try {
			
			parser.parseArgument(args);
		
		} catch (CmdLineException e) {

			System.err.println(e.getMessage());
			System.err.print("Arguments: ");
			parser.printSingleLineUsage(System.err);
			System.err.println("\n\n Options: \n");
			parser.printUsage(System.err);
			System.exit(-1);
		
		} 
				
		if( !options.pdfFileOrDir.exists() ) {
			System.err.println(options.pdfFileOrDir + " does not exist.");
			System.err.print("Arguments: ");
			parser.printSingleLineUsage(System.err);
			System.err.println("\n\n Options: \n");
			parser.printUsage(System.err);
			System.exit(-1);
		}
		
		DigitalLibraryEngine de = null;
		
		if (options.pdfRuleFile != null) {
			logger.info("Using rulefile " + options.pdfRuleFile.getPath());
			de = new DigitalLibraryEngine(options.pdfRuleFile);
		} else {
			de = new DigitalLibraryEngine();
		}		
		de.initializeVpdmfDao(options.login, 
				options.password, 
				options.dbName, 
				options.workingDirectory);
				
		de.updatePmidPdfFileOrDir(options.pdfFileOrDir, 
				options.forceBlocking);

	}

}
