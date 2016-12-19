package edu.isi.bmkeg.digitalLibrary.bin.bigMech;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.isi.bmkeg.digitalLibrary.controller.DigitalLibraryEngine;
import edu.isi.bmkeg.ftd.model.qo.FTD_qo;
import edu.isi.bmkeg.utils.Converters;
import edu.isi.bmkeg.vpdmf.model.instances.LightViewInstance;

public class ExtractCodedFragmentsAsClauses {

	public static class Options {

		@Option(name = "-outPath", usage = "Output File", required = true, metaVar = "OUTPUT")
		public File outPath;
		
		@Option(name = "-corpus", usage = "Corpus", required = true, metaVar = "CORPUS")
		public String corpus = "";

		@Option(name = "-frgType", usage = "FrgType", required = true, metaVar = "FRG_TYPE")
		public String frgType = "";
		
		@Option(name = "-l", usage = "Database login", required = true, metaVar = "LOGIN")
		public String login = "";

		@Option(name = "-p", usage = "Database password", required = true, metaVar = "PASSWD")
		public String password = "";

		@Option(name = "-db", usage = "Database name", required = true, metaVar = "DBNAME")
		public String dbName = "";

		@Option(name = "-wd", usage = "Working directory", required = true, metaVar = "WDIR")
		public String workingDirectory = "";

	}

	private static Logger logger = Logger.getLogger(ExtractCodedFragmentsAsClauses.class);

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		Options options = new Options();
		Pattern patt = Pattern.compile("(\\S+)-fig(\\S+)");
		
		Map<Integer,Set<String>> pmids = new HashMap<Integer,Set<String>>();
		Map<Integer,String> pmcids = new HashMap<Integer,String>();
		Map<String,String> pdfLocs = new HashMap<String,String>();
		
		CmdLineParser parser = new CmdLineParser(options);
	
		int nLapdfErrors = 0, nSwfErrors = 0, total = 0;
		
		try {

			parser.parseArgument(args);
						
			DigitalLibraryEngine de = new DigitalLibraryEngine();
			de.initializeVpdmfDao(
					options.login, 
					options.password, 
					options.dbName, 
					options.workingDirectory);	
			
			// Query based on a query constructed with SqlQueryBuilder based on the TriagedArticle view.
			String countSql = "SELECT COUNT(*) ";

			String selectSql = "SELECT l.vpdmfId, a.pmid, f.name, frg.vpdmfId, frg.frgOrder, blk.vpdmfOrder, blk.text, blk.code ";
			
			String fromWhereSql = "FROM LiteratureCitation AS l," +
					" ArticleCitation as a, FTD as f, " + 
					" FTDFragment as frg, FTDFragmentBlock as blk, " +
					" Corpus_corpora__resources_LiteratureCitation AS link, " + 
					" Corpus AS c " +
					" WHERE " +
					"blk.fragment_id = frg.vpdmfId AND " +
					"l.fullText_id = f.vpdmfId AND " +
					"l.vpdmfId = a.vpdmfId AND " +
					"frg.ftd_id = f.vpdmfId AND " +
					"link.resources_id=l.vpdmfId AND " +
					"link.corpora_id=c.vpdmfId AND " +
					"c.name = '"+ options.corpus+ "' AND " + 
					"frg.frgType = '"+ options.frgType + "' " +
					" ORDER BY l.vpdmfId, frg.vpdmfId, frg.frgOrder, blk.vpdmfOrder;";

			de.getDigLibDao().getCoreDao().getCe().connectToDB();
			
			ResultSet countRs = de.getDigLibDao().getCoreDao().getCe().executeRawSqlQuery(
					countSql + fromWhereSql);
			countRs.next();
			int count = countRs.getInt(1);
						
			countRs.close();
			
			ResultSet rs = de.getDigLibDao().getCoreDao().getCe().executeRawSqlQuery(
					selectSql + fromWhereSql);

			Map<Long,Map<String,String>> lookup = new HashMap<Long,Map<String,String>>();

			Map<String,String> hash = new HashMap<String,String>();
			
			File output = new File( options.outPath.getPath() );
			BufferedWriter writer = new BufferedWriter(new FileWriter(output));
			
			while( rs.next() ) {

				Long citationId = rs.getLong("l.vpdmfId");
				String fileStem = rs.getString("f.name");
				int pmid = rs.getInt("a.pmid");
				String frgOrder = rs.getString("frg.frgOrder");
				String blkId = rs.getString("blk.vpdmfOrder");
				String code = rs.getString("blk.code");
				String text = rs.getString("blk.text");

				writer.write( pmid+"\t"+frgOrder+"\t"+blkId+"\t"+code+"\t"+text+"\n");
				
			}
			rs.close();
			writer.close();

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
