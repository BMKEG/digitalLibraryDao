package edu.isi.bmkeg.digitalLibrary.bin;

import java.io.File;
import java.util.Map;

import org.apache.log4j.Logger;

import edu.isi.bmkeg.digitalLibrary.controller.DigitalLibraryEngine;
import edu.isi.bmkeg.digitalLibrary.controller.medline.VpdmfMedlineHandler;
import edu.isi.bmkeg.digitalLibrary.model.citations.Journal;
import edu.isi.bmkeg.digitalLibrary.utils.JournalLookupPersistentObject;

public class ImportMedlineDirectory {

	private static Logger logger = Logger.getLogger(ImportMedlineDirectory.class);

	public static String USAGE = "arguments: <local-dir> <login> <password> <dbName>"; 
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
				
		if( args.length != 4 ) {
			System.err.println(USAGE);
			System.exit(-1);
		}

		File fileDir = new File(args[0]);				
				
		String login = args[1];
		String password = args[2];
		String dbName = args[3];;				
		
		DigitalLibraryEngine de = new DigitalLibraryEngine();
		de.initializeVpdmfDao(login, password, dbName);
		
		de.loadMedlineArchiveDirectory(fileDir);
				
	}

}
