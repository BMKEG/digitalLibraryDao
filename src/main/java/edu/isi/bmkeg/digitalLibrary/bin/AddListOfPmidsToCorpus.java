package edu.isi.bmkeg.digitalLibrary.bin;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.isi.bmkeg.digitalLibrary.controller.DigitalLibraryEngine;

public class AddListOfPmidsToCorpus {

	private static Logger logger = Logger.getLogger(AddListOfPmidsToCorpus.class);

	private static Pattern patt = Pattern.compile("\\b(\\d+)\\b");
	
	public static class Options {

		@Option(name = "-pmidsFile", usage = "File containing list of PMIDs", required = true, metaVar = "PMID-LIST")
		public File pmidsFile;
		
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

			if( !options.pmidsFile.exists() ) {
				throw new CmdLineException(parser, options.pmidsFile.getAbsolutePath() + " does not exist.");
			}
			
			de = new DigitalLibraryEngine();
			de.initializeVpdmfDao(options.login, options.password, options.dbName, options.workingDirectory);
			de.getDigLibDao().getCoreDao().connectToDb();
						
			BufferedReader in = new BufferedReader(new InputStreamReader(
					new FileInputStream(options.pmidsFile)));			
			String inputLine;
			List<Integer> ids = new ArrayList<Integer>();
			while ((inputLine = in.readLine()) != null) {
				Matcher m = patt.matcher(inputLine);
				if( m.find() ) {
					Integer pmid = new Integer(m.group(1)); 
					ids.add(pmid);					
				}
			}
			Collections.sort(ids);
			de.loadArticlesFromPmidListToCorpus(ids, options.corpusName);		
			
		} catch (CmdLineException e) {

			System.err.println(e.getMessage());
			System.err.print("Arguments: ");
			parser.printSingleLineUsage(System.err);
			System.err.println("\n\n Options: \n");
			parser.printUsage(System.err);
			System.exit(-1);
		
		} finally {
			
			de.getDigLibDao().getCoreDao().commitTransaction();
			
		}

	}

}
