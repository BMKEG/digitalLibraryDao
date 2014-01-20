package edu.isi.bmkeg.digitalLibrary.bin;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.isi.bmkeg.digitalLibrary.controller.DigitalLibraryEngine;
import edu.isi.bmkeg.digitalLibrary.model.citations.Corpus;
import edu.isi.bmkeg.digitalLibrary.utils.pubmed.ESearcher;
import edu.isi.bmkeg.vpdmf.model.definitions.VPDMf;

public class BuildCorpusFromMedlineQuery {

	public static String USAGE = "arguments: <name> <queryString> " 
			+ "<dbName> <login> <password>";

	private static Logger logger = Logger.getLogger(BuildCorpusFromMedlineQuery.class);

	private VPDMf top;
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		if( args.length != 5 ) {
			System.err.println(USAGE);
			System.exit(-1);
		}

		String corpusName = args[0];
		String queryString = args[1];

		String dbName = args[2];
		String login = args[3];
		String password = args[4];
		
		DigitalLibraryEngine dlEng = new DigitalLibraryEngine ();
		dlEng.initializeVpdmfDao(login, password, dbName);
		
		try {
			
			dlEng.getDigLibDao().getCoreDao().connectToDb();
			
			ESearcher eSearcher = new ESearcher(queryString);
			int maxCount = eSearcher.getMaxCount();
			Set<Integer> esearchIds = new HashSet<Integer>();
			for(int i=0; i<maxCount; i=i+1000) {
	
				long t = System.currentTimeMillis();
				
				esearchIds.addAll( eSearcher.executeESearch(i, 1000) );
				
				long deltaT = System.currentTimeMillis() - t;
				logger.info("esearch 1000 entries: " + deltaT / 1000.0
						+ " s\n");
				
				logger.info("wait 3 secs");
				Thread.sleep(3000);
			}
	
			Corpus c = new Corpus();
			
			c.setName(corpusName);
			c.setDescription(queryString);
			Date d = new Date();
			c.setDate(d.toString());
			
			//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
			// insert the corpus
			//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
			long t = System.currentTimeMillis();
			dlEng.getDigLibDao().getCoreDao().insertInTrans(c, "ArticleCorpus");
			long deltaT = System.currentTimeMillis() - t;
			logger.info("inserting corpus '"+corpusName+"': "+deltaT/1000.0+" s\n");
			
			//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
			// insert the articles
			//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
			t = System.currentTimeMillis();
			dlEng.insertArticlesFromPmidList_inTrans(esearchIds);
			deltaT = System.currentTimeMillis() - t;
			logger.info("inserting corpus '"+corpusName+"': "+deltaT/1000.0+" s\n");
			
			//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
			// add articles to the corpus... probably should not be transactional, 
			// make this interruptable and restartable since it's likely to be quite 
			// slow. OK. How to develop a batch-upload function for collections?
			//
			// Need to make this a batch load function. 
			//
			//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
			t = System.currentTimeMillis();
			dlEng.loadArticlesFromPmidListToCorpus(esearchIds, corpusName);
			deltaT = System.currentTimeMillis() - t;
			logger.info("linking corpus and articles: "+deltaT/1000.0+" s\n");

			dlEng.getDigLibDao().getCoreDao().commitTransaction();

		} catch (Exception e) {
			
			e.printStackTrace();
			dlEng.getDigLibDao().getCoreDao().rollbackTransaction();

		}
		
		dlEng.getDigLibDao().getCoreDao().closeDbConnection();

		
	}

}
