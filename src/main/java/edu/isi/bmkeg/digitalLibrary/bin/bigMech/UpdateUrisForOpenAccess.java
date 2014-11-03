package edu.isi.bmkeg.digitalLibrary.bin.bigMech;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
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
import edu.isi.bmkeg.digitalLibrary.model.qo.citations.ArticleCitation_qo;
import edu.isi.bmkeg.digitalLibrary.model.qo.citations.Corpus_qo;
import edu.isi.bmkeg.vpdmf.model.definitions.VPDMf;
import edu.isi.bmkeg.vpdmf.model.instances.LightViewInstance;


/**
 * This command-line utility runs through all papers in a corpus and checks it against
 * a file of OA PMC identifiers. If it finds a paper in the list, it updates the URL of that 
 * paper in the database.
 * 
 * @author burns
 *
 */
public class UpdateUrisForOpenAccess {

	public static class Options {
		
		@Option(name = "-pmcMapFile", usage = "PMC mapping file", required = true, metaVar = "PMC")
		public File pmcMapFile;
		
		@Option(name = "-pdfLocFile", usage = "pdf Location file", required = true, metaVar = "PDF")
		public File ftpPdfLocFile;
		
		@Option(name = "-corpus", usage = "Corpus name", required = true, metaVar = "CORPUS")
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

	private static Logger logger = Logger.getLogger(UpdateUrisForOpenAccess.class);

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		Options options = new Options();
		Map<String,Integer> pmcIdMap = new HashMap<String,Integer>();
		Map<Integer,String> pmidMap = new HashMap<Integer,String>();
		Map<String,String> pdfLocs = new HashMap<String,String>();
		
		CmdLineParser parser = new CmdLineParser(options);
		
		try {

			parser.parseArgument(args);

			BufferedReader input = new BufferedReader(new FileReader(
						options.pmcMapFile));
			String line = input.readLine(); // 1st line are column headings 
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
			
			input.close();
			input = new BufferedReader(new FileReader(
					options.ftpPdfLocFile));
			
			try {
					
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
						
		//
		// UPLOAD LITERATURE CITATIONS 
		//
		DigitalLibraryEngine de = new DigitalLibraryEngine();
		de.initializeVpdmfDao(options.login, options.password, options.dbName, options.workingDirectory);
		de.getDigLibDao().getCoreDao().connectToDb();
		VPDMf top = de.getDigLibDao().getCoreDao().getTop();
		
		if( options.corpus != null ) {
			
			Corpus_qo cQo = new Corpus_qo();
			cQo.setName(options.corpus);
			ArticleCitation_qo aQo = new ArticleCitation_qo();
			aQo.getCorpora().add(cQo);
			
			List<LightViewInstance> list = de.getDigLibDao().getCoreDao().listInTrans(aQo, "ArticleCitation");
			
			for( LightViewInstance lvi : list ) {
				
				Map<String,String> obj = lvi.readIndexTupleMap(top);
				String pmidString = obj.get("[ArticleCitation]LiteratureCitation|ArticleCitation.pmid");
				String pmcidString = pmidMap.get(new Integer(pmidString));
				
				if( pdfLocs.containsKey( pmcidString )) {
					ArticleCitation ac = de.getDigLibDao().getCoreDao().findByIdInTrans(
							lvi.getVpdmfId(), new ArticleCitation(), "ArticleCitation");
					edu.isi.bmkeg.digitalLibrary.model.citations.URL u = 
							new edu.isi.bmkeg.digitalLibrary.model.citations.URL();
					u.setResource(ac);
					u.setUrl("ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/" + pdfLocs.get(pmcidString) );
					ac.getFullTextUrl().add(u);
					de.getDigLibDao().getCoreDao().updateInTrans(ac, "ArticleCitation");					
				}
				
			}
			
		}
		
		de.getDigLibDao().getCoreDao().commitTransaction();
		
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
