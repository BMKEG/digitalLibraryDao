package edu.isi.bmkeg.digitalLibrary.bin;

import java.io.File;
import java.util.Map;

import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.isi.bmkeg.digitalLibrary.bin.BuildDigitalLibraryDatabase.Options;
import edu.isi.bmkeg.digitalLibrary.controller.DigitalLibraryEngine;
import edu.isi.bmkeg.digitalLibrary.model.citations.Journal;
import edu.isi.bmkeg.digitalLibrary.utils.JournalLookupPersistentObject;
import edu.isi.bmkeg.digitalLibrary.utils.pubmed.VpdmfMedlineHandler;

public class ImportMedlineDirectory {

	public static class Options {

		@Option(name = "-fileDir", usage = "Directory of Medline Files", required = true, metaVar = "DIR")
		public File fileDir;
		
		@Option(name = "-l", usage = "Database login", required = true, metaVar = "LOGIN")
		public String login = "";

		@Option(name = "-p", usage = "Database password", required = true, metaVar = "PASSWD")
		public String password = "";

		@Option(name = "-db", usage = "Database name", required = true, metaVar  = "DBNAME")
		public String dbName = "";
		
		@Option(name = "-wd", usage = "Working directory", required = true, metaVar  = "WDIR")
		public String workingDirectory = "";
		
	}
	
	private static Logger logger = Logger.getLogger(ImportMedlineDirectory.class);

	public static String USAGE = "arguments: <local-dir> <login> <password> <dbName> <workingDirectory>"; 
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
				
		Options options = new Options();
		
		CmdLineParser parser = new CmdLineParser(options);

		try {

			parser.parseArgument(args);
			
		DigitalLibraryEngine de = new DigitalLibraryEngine();
		de.initializeVpdmfDao(options.login, 
				options.password, 
				options.dbName, options.workingDirectory);
		
		de.loadMedlineArchiveDirectory(options.fileDir);
				
		} catch (CmdLineException e) {

			System.err.println(e.getMessage());
			System.err.print("Arguments: ");
			parser.printSingleLineUsage(System.err);
			System.err.println("\n\n Options: \n");
			parser.printUsage(System.err);
			System.exit(-1);
		
		} catch (Exception e2) {
		
			e2.printStackTrace();
		
		}
	
	}

}
