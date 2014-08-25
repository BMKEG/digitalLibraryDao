package edu.isi.bmkeg.digitalLibrary.bin.bigMech;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
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
public class ImportCitationsFromPmcSet {

	public static class Options {

		@Option(name = "-file", usage = "Path to PMC Dump File", required = true, metaVar = "PMIDS")
		public File file;
		
		@Option(name = "-pmcMapFile", usage = "PMC mapping file", required = false, metaVar = "PMC")
		public File pmcMapFile;

		@Option(name = "-pdfLocFile", usage = "pdf Location file", required = false, metaVar = "PDF")
		public File ftpPdfLocFile;
		
		@Option(name = "-corpus", usage = "Corpus name", required = false, metaVar = "CORPUS")
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

	private static Logger logger = Logger.getLogger(ImportCitationsFromPmcSet.class);

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		Options options = new Options();
		Map<String,Integer> pmcIds = new HashMap<String,Integer>();
		Map<String,Integer> pmcIdMap = new HashMap<String,Integer>();
		Map<Integer,String> pmidMap = new HashMap<Integer,String>();
		Map<String,String> pdfLocs = new HashMap<String,String>();
		
		CmdLineParser parser = new CmdLineParser(options);
		
		try {

			parser.parseArgument(args);

			BufferedReader input = new BufferedReader(new FileReader(
					options.file));
			
			try {
			
				String line = null;
				
				/* readLine returns the content of a line MINUS the newline. 
				 * It returns null only for the END of the stream. 
				 * it returns an empty String if two newlines appear in
				 * a row.
				 * 
				 * One entry per line for each PMID. 
				 */
				int count = 0;
				while ((line = input.readLine()) != null) {
					if( line.startsWith("PMC") ) {
						if( pmcIds.keySet().contains(line) ) {
							Integer i = pmcIds.get(line);
							pmcIds.put(line, i++);
						} else {
							pmcIds.put(line, 1);							
						}
						
					}
					count++;
				}
				
				logger.info("Input count: " + count);
				logger.info("Unique ids: " + pmcIds.size());
				
				if( options.pmcMapFile != null ) {
					
					input.close();
					input = new BufferedReader(new FileReader(
							options.pmcMapFile));
					line = input.readLine(); // 1st line are column headings 
					LINELOOP: while ((line = input.readLine()) != null) {
						String[] lineArray = line.split(",");
						
						for(int i=lineArray.length-1; i>=0; i--) {
							if( lineArray[i].startsWith("PMC") ) {
								String pmcId = lineArray[i];
								Integer pmid;
								try {
									pmid = new Integer(lineArray[i+1]);
								} catch (Exception e) {
									//logger.error(line);
									continue;
								}						
								pmcIdMap.put(pmcId,pmid);
								pmidMap.put(pmid,pmcId);
								continue LINELOOP;
							}
						}
					}
				}
				
				if( options.ftpPdfLocFile != null ) {
					
					input.close();
					input = new BufferedReader(new FileReader(
							options.ftpPdfLocFile));
					line = null;
					while ((line = input.readLine()) != null) {
						String[] lineArray = line.split("\\t");
							
						if( lineArray.length != 3) {
							continue;
						}
						
						String pdfLoc = lineArray[0];
						String pmcId = lineArray[2];
						pdfLocs.put(pmcId, pdfLoc);

					}
				}
				
			} finally {
				input.close();
			}

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
		
		List<String> sortedPmcIds = new ArrayList<String>(pmcIds.keySet());
		//Collections.sort(sortedPmids);
		
		//
		// REPORTS 
		//
		int pmcCount = 0, pdfCount = 0;
		Map<Integer,String> ftdLocations = new HashMap<Integer,String>();
		List<Integer> noPdfPmids = new ArrayList<Integer>();
		for( String pmcId : sortedPmcIds) {
			if( pmcIdMap.containsKey(pmcId) ) {
				pmcCount++;
				Integer pmid = pmcIdMap.get(pmcId);
				if( pdfLocs.containsKey(pmcId) ) {
					pdfCount++;			
					ftdLocations.put(pmid, 
							"ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/" +
									pdfLocs.get(pmcId)
					);
				} else {
					noPdfPmids.add(pmid);
				}
			}
		}
		
		List<Integer> pmidsWithPdfs = new ArrayList<Integer>(ftdLocations.keySet());
		
		System.out.println("Number of selected articles in PMC:" + pmcCount);
		System.out.println("Number of selected articles with open access PDFs:" + pdfCount);
		System.out.println("Number of selected articles WITHOUT open access PDFs:" + noPdfPmids.size());
		
		//
		// UPLOAD LITERATURE CITATIONS 
		//
		DigitalLibraryEngine de = new DigitalLibraryEngine();
		de.initializeVpdmfDao(options.login, options.password, options.dbName, options.workingDirectory);
		de.getDigLibDao().getCoreDao().connectToDb();		
		
		List<ArticleCitation> noPdfsAcs = de.insertArticlesFromPmidList(noPdfPmids, ftdLocations);
		List<ArticleCitation> inAcs = de.insertArticlesFromPmidList(pmidsWithPdfs, ftdLocations);
		
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
			
			de.getExtDigLibDao().addArticlesToCorpus(noPdfPmids, options.corpus);
			de.getExtDigLibDao().addArticlesToCorpus(pmidsWithPdfs, options.corpus);
			
		}
		
		
	}

	private static Set<String> readSetOfStrings(String str) {
		String[] strArray = str.split(":");
		Set<String> strSet = new HashSet<String>();
		for(String s : strArray) { 
			strSet.add(s); 
		}
		return strSet;
	}
	
	
}
