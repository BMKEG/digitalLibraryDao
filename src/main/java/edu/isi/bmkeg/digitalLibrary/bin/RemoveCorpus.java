package edu.isi.bmkeg.digitalLibrary.bin;

import org.apache.log4j.Logger;

import edu.isi.bmkeg.digitalLibrary.controller.DigitalLibraryEngine;
import edu.isi.bmkeg.vpdmf.model.definitions.VPDMf;

public class RemoveCorpus {

	public static String USAGE = "arguments: <corpusName> <dbName> <login> <password>"; 

	private static Logger logger = Logger.getLogger(RemoveCorpus.class);

	private VPDMf top;
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		if( args.length != 4) {
			System.err.println(USAGE);
			System.exit(-1);
		}

		String corpusName = args[0];	
		String dbName = args[1];
		String login = args[2];
		String password = args[3];
		
		DigitalLibraryEngine de = new DigitalLibraryEngine();
		de.initializeVpdmfDao(login, password, dbName);
		
		de.deleteCorpus(corpusName);

	}

}
