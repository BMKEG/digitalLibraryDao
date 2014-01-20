package edu.isi.bmkeg.digitalLibrary.bin;

import java.util.List;
import java.util.regex.Matcher;

import org.apache.log4j.Logger;

import edu.isi.bmkeg.digitalLibrary.controller.DigitalLibraryEngine;
import edu.isi.bmkeg.digitalLibrary.model.citations.Journal;
import edu.isi.bmkeg.digitalLibrary.model.citations.JournalEpoch;
import edu.isi.bmkeg.digitalLibrary.model.qo.citations.Corpus_qo;
import edu.isi.bmkeg.digitalLibrary.model.qo.citations.JournalEpoch_qo;
import edu.isi.bmkeg.ftd.model.FTDRuleSet;
import edu.isi.bmkeg.vpdmf.model.definitions.VPDMf;
import edu.isi.bmkeg.vpdmf.model.instances.LightViewInstance;

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
		
		Long id = -1L;

		try {

			de.getDigLibDao().getCoreDao().getCe().connectToDB();

			Corpus_qo cQo = new Corpus_qo();
			cQo.setName(corpusName);
 			List<LightViewInstance> l = de.getDigLibDao().listCorpus(cQo);
 			
 			if( l.size() == 1 ) {
 				de.getDigLibDao().deleteCorpusById(l.get(0).getVpdmfId());
			}

		} catch (Exception e) {

			e.printStackTrace();
			throw e;

		} finally {

			de.getDigLibDao().getCoreDao().getCe().closeDbConnection();

		}

	}

}
