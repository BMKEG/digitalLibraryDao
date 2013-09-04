package edu.isi.bmkeg.digitalLibrary.bin;

import java.io.File;

import org.apache.log4j.Logger;

import edu.isi.bmkeg.digitalLibrary.controller.DigitalLibraryEngine;
import edu.isi.bmkeg.vpdmf.model.definitions.VPDMf;

public class UpdatePmidEncodedPdfs {

	public static String USAGE = "arguments: <pdf-dir-or-file> <dbName> <login> <password> [<rule-file>]"; 

	private static Logger logger = Logger.getLogger(UpdatePmidEncodedPdfs.class);

	private VPDMf top;
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		if (args.length < 4 || args.length > 5 ) {
			System.err.println(USAGE);
			System.exit(1);
		}
		
		File pdfFileOrDr = new File(args[0]);

		if( !pdfFileOrDr.exists() ) {
			System.err.println(args[0] + " does not exist.");
			System.exit(-1);
		}
	
		String dbName = args[1];
		String login = args[2];
		String password = args[3];
		String ruleFileLocation = null;
		
		if (args.length == 5) 
			ruleFileLocation = args[4];
		
		DigitalLibraryEngine de = null;
		
		if (ruleFileLocation != null) {
			logger.info("Using rulefile " + ruleFileLocation);
			de = new DigitalLibraryEngine(new File(ruleFileLocation));
		} else {
			de = new DigitalLibraryEngine();
		}		
		de.initializeVpdmfDao(login, password, dbName);
		
		de.updatePmidPdfFileOrDir(pdfFileOrDr);

	}

}
