package edu.isi.bmkeg.digitalLibrary.bin;

import java.io.File;

import org.apache.log4j.Logger;

import edu.isi.bmkeg.digitalLibrary.controller.DigitalLibraryEngine;
import edu.isi.bmkeg.vpdmf.model.definitions.VPDMf;

public class RemoveArticleCitationFromCorpus {

	public static String USAGE = "arguments: <pmid-file> <corpus-name> <dbName> <login> <password>"; 

	private static Logger logger = Logger.getLogger(RemoveArticleCitationFromCorpus.class);

	private VPDMf top;
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		if( args.length != 5) {
			System.err.println(USAGE);
			System.exit(-1);
		}

		File pmidFile = new File(args[0]);

		if( !pmidFile.exists() ) {
			System.err.println(USAGE);
			System.exit(-1);
		}
	
		String corpusName = args[1];
		String dbName = args[2];
		String login = args[3];
		String password = args[4];
		
		DigitalLibraryEngine de = new DigitalLibraryEngine();
		de.initializeVpdmfDao(login, password, dbName);
		
		de.deleteArticleCitationsFromCorpus(pmidFile, corpusName);

	}

}
