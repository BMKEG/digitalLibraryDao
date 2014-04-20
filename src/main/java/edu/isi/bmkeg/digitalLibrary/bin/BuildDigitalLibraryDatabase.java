package edu.isi.bmkeg.digitalLibrary.bin;

import java.io.File;
import java.net.URL;

import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.isi.bmkeg.vpdmf.bin.BuildDatabaseFromVpdmfArchive;
import edu.isi.bmkeg.vpdmf.model.definitions.VPDMf;

public class BuildDigitalLibraryDatabase {

	public static String USAGE = "arguments: <dbName> <login> <password>"; 

	private static Logger logger = Logger.getLogger(BuildDigitalLibraryDatabase.class);

	public static class Options {
		
		@Option(name = "-l", usage = "Database login", required = true, metaVar = "LOGIN")
		public String login = "";

		@Option(name = "-p", usage = "Database password", required = true, metaVar = "PASSWD")
		public String password = "";

		@Option(name = "-db", usage = "Database name", required = true, metaVar  = "DBNAME")
		public String dbName = "";
		
	}

	
	private VPDMf top;
	
	public static void main(String[] args) {

		Options options = new Options();
		
		CmdLineParser parser = new CmdLineParser(options);

		try {

			parser.parseArgument(args);

			URL url = ClassLoader.getSystemClassLoader().getResource("edu/isi/bmkeg/digitalLibrary/digitalLibrary-mysql.zip");
			String buildFilePath = url.getFile();
			File buildFile = new File( buildFilePath );

			String[] newArgs = new String[] { 
					buildFile.getPath(), options.dbName, options.login, options.password 
					};
			
			BuildDatabaseFromVpdmfArchive.main(newArgs);
						
			logger.info("Digital Library Database " + options.dbName + " successfully created.");
				
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
