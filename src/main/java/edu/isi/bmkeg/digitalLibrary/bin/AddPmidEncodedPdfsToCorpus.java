package edu.isi.bmkeg.digitalLibrary.bin;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.isi.bmkeg.digitalLibrary.controller.DigitalLibraryEngine;
import edu.isi.bmkeg.digitalLibrary.dao.vpdmf.VpdmfCitationsDao;
import edu.isi.bmkeg.digitalLibrary.model.citations.ArticleCitation;
import edu.isi.bmkeg.digitalLibrary.model.citations.ID;
import edu.isi.bmkeg.digitalLibrary.model.citations.Journal;
import edu.isi.bmkeg.digitalLibrary.utils.JournalLookupPersistentObject;
import edu.isi.bmkeg.digitalLibrary.utils.pubmed.EFetcher;
import edu.isi.bmkeg.lapdf.controller.LapdfEngine;
import edu.isi.bmkeg.vpdmf.dao.CoreDaoImpl;
import edu.isi.bmkeg.vpdmf.model.definitions.VPDMf;

public class AddPmidEncodedPdfsToCorpus {

	public static String USAGE = "arguments: <pdf-dir-or-file> <corpus-name> <dbName> <login> <password> [<rule-file>]"; 

	private static Logger logger = Logger.getLogger(AddPmidEncodedPdfsToCorpus.class);

	private VPDMf top;
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		if (args.length < 5 || args.length > 6 ) {
			System.err.println(USAGE);
			System.exit(1);
		}
		
		File pdfFileOrDr = new File(args[0]);

		if( !pdfFileOrDr.exists() ) {
			System.err.println(args[0] + " does not exist.");
			System.exit(-1);
		}
	
		String corpusName = args[1];
		String dbName = args[2];
		String login = args[3];
		String password = args[4];
		String ruleFileLocation = null;
		
		if (args.length == 6) 
			ruleFileLocation = args[5];
		
		DigitalLibraryEngine de = null;
		
		if (ruleFileLocation != null) {
			logger.info("Using rulefile " + ruleFileLocation);
			de = new DigitalLibraryEngine(new File(ruleFileLocation));
		} else {
			de = new DigitalLibraryEngine();
		}		
		de.initializeVpdmfDao(login, password, dbName);
		
		Map<Integer,Long> mapPmidsToVpdmfids = de.insertPmidPdfFileOrDir(pdfFileOrDr);
		
		de.loadArticlesFromPmidListToCorpus(mapPmidsToVpdmfids.keySet(), corpusName);		

	}

}
