package edu.isi.bmkeg.digitalLibrary.bin;

import java.io.File;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.isi.bmkeg.digitalLibrary.bin.AddPmidEncodedPdfsToCorpus.Options;
import edu.isi.bmkeg.digitalLibrary.controller.DigitalLibraryEngine;
import edu.isi.bmkeg.digitalLibrary.model.citations.Corpus;
import edu.isi.bmkeg.digitalLibrary.utils.pubmed.ESearcher;
import edu.isi.bmkeg.vpdmf.model.definitions.VPDMf;

public class BuildCorpusFromMedlineQuery {

	public static String USAGE = "arguments: <name> <queryString> " 
			+ "<dbName> <login> <password> <workingDirectory>";

	private static Logger logger = Logger.getLogger(BuildCorpusFromMedlineQuery.class);

	private VPDMf top;
	
	public static class Options {

		@Option(name = "-query", usage = "Medline query", required = true, metaVar = "PDF-DIR-OR-FILE")
		public String queryString;
		
		@Option(name = "-corpus", usage = "Corpus name", required = false, metaVar = "CORPUS")
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

		DigitalLibraryEngine dlEng = new DigitalLibraryEngine ();
		
		try {

			parser.parseArgument(args);

			String corpusName = options.corpusName;
			String queryString = options.queryString;
	
			String dbName = options.dbName;
			String login = options.login;
			String password = options.password;
			String workingDirectory = options.workingDirectory;
			
			dlEng.initializeVpdmfDao(login, password, dbName, workingDirectory);			
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

		} catch (CmdLineException e) {

			System.err.println(e.getMessage());
			System.err.print("Arguments: ");
			parser.printSingleLineUsage(System.err);
			System.err.println("\n\n Options: \n");
			parser.printUsage(System.err);
			System.exit(-1);
		
		} catch (Exception e2) {

			e2.printStackTrace();
			dlEng.getDigLibDao().getCoreDao().rollbackTransaction();
			
		} finally {
		
			dlEng.getDigLibDao().getCoreDao().closeDbConnection();
		
		}
		
	}

}
