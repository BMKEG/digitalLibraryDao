package edu.isi.bmkeg.digitalLibrary.bin.bigMech;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.select.Elements;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.isi.bmkeg.digitalLibrary.controller.DigitalLibraryEngine;
import edu.isi.bmkeg.digitalLibrary.model.citations.ArticleCitation;
import edu.isi.bmkeg.digitalLibrary.model.citations.Corpus;
import edu.isi.bmkeg.digitalLibrary.model.qo.citations.Corpus_qo;
import edu.isi.bmkeg.vpdmf.model.instances.LightViewInstance;


/**
 * This command-line utility loads a file of OA PMC identifiers, loads their citations, 
 * downloads PDFs and XML data for each file
 * 
 * @author burns
 *
 */
public class GetPmidsFromPathwayLogic {

	public Connection dbConnection;
	protected Set<String> drivers = new HashSet<String>();
	protected Statement stat;
	protected Statement uStat;
	
	public static class Options {

		@Option(name = "-html", usage = "HTML File", required = true, metaVar = "HTML")
		public File htmlFile;
		
		@Option(name = "-pdfLocFile", usage = "pdf Location file", required = false, metaVar = "PDF")
		public File ftpPdfLocFile;
		
		@Option(name = "-corpus", usage = "Corpus", required = false, metaVar = "CORPUS")
		public String corpus;

		@Option(name = "-l", usage = "Database login", required = true, metaVar = "LOGIN")
		public String login = "";

		@Option(name = "-p", usage = "Database password", required = true, metaVar = "PASSWD")
		public String password = "";

		@Option(name = "-db", usage = "Database name", required = true, metaVar = "DBNAME")
		public String dbName = "";

		@Option(name = "-wd", usage = "Working directory", required = true, metaVar = "WDIR")
		public String workingDirectory = "";

	}

	private static Logger logger = Logger.getLogger(GetPmidsFromPathwayLogic.class);

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		Options options = new Options();
		Pattern patt = Pattern.compile("source:\\s+\\<a.*\\>(\\d+)\\<\\/a\\>");
		Map<String,String> pdfLocs = new HashMap<String,String>();
		
		CmdLineParser parser = new CmdLineParser(options);

		try {

			parser.parseArgument(args);
			
			Set<Integer> pmidSet = new HashSet<Integer>();
			
			FileReader fr = new FileReader(options.htmlFile);
			BufferedReader br = new BufferedReader(fr);
			String  thisLine = null;
	        while ((thisLine = br.readLine()) != null) {
	        	Matcher m = patt.matcher(thisLine);
	        	if( m.find() ) {
					Integer pmid = new Integer(m.group(1));
					pmidSet.add(pmid);
				}
	        }       
					
			List<Integer> pmids = new ArrayList<Integer>(pmidSet);
	        
			DigitalLibraryEngine de = new DigitalLibraryEngine();
			de.initializeVpdmfDao(options.login, options.password, options.dbName, options.workingDirectory);
			de.getDigLibDao().getCoreDao().connectToDb();		
			
			Map<Integer, String> ftdLocations = new HashMap<Integer, String>();
			List<ArticleCitation> acs = de.insertArticlesFromPmidList(pmids, ftdLocations );
			
			if( options.corpus != null ) {
				
				Corpus_qo cQo = new Corpus_qo();
				cQo.setName(options.corpus);
				
				List<LightViewInstance> list = de.getDigLibDao().getCoreDao().list(cQo, "ArticleCorpus");
				
				if( list.size() == 0 ) {
					Corpus c = new Corpus();
					c.setCorpusType("ArticleCorpus");
					c.setName(options.corpus);
					c.setRegex(options.corpus.substring(0, 1).toUpperCase());
					de.getDigLibDao().getCoreDao().insert(c, "ArticleCorpus");
				}
				
				de.getExtDigLibDao().addArticlesToCorpus(pmids, options.corpus);
				
			}
			
			logger.info("Finished");
			
		} catch (CmdLineException e) {

			System.err.println(e.getMessage());
			System.err.print("Arguments: ");
			parser.printSingleLineUsage(System.err);
			System.err.println("\n\n Options: \n");
			parser.printUsage(System.err);
			System.exit(-1);

		} catch (Exception e2) {

			e2.printStackTrace();

		}
	
	}
	
}
