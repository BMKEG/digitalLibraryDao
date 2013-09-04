package edu.isi.bmkeg.digitalLibrary.bin;

import java.io.File;
import java.util.Map;

import org.apache.log4j.Logger;

import edu.isi.bmkeg.digitalLibrary.controller.DigitalLibraryEngine;
import edu.isi.bmkeg.vpdmf.model.definitions.VPDMf;

public class AddArticleCitations {

	public static String USAGE = "arguments: <pmid-file> <dbName> <login> <password>"; 

	private static Logger logger = Logger.getLogger(AddArticleCitations.class);

	private VPDMf top;
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		if( args.length != 4) {
			System.err.println(USAGE);
			System.exit(-1);
		}

		File pmidFile = new File(args[0]);

		if( !pmidFile.exists() ) {
			System.err.println(USAGE);
			System.exit(-1);
		}
	
		String dbName = args[1];
		String login = args[2];
		String password = args[3];
		
		DigitalLibraryEngine de = new DigitalLibraryEngine();
		de.initializeVpdmfDao(login, password, dbName);
		
		// Load existing pmids from database
		Map<Integer, Long> pmidMap = de.buildPmidLookupFromDb();
		de.insertArticlesFromPmidList(pmidFile, pmidMap.keySet());

	}

}
