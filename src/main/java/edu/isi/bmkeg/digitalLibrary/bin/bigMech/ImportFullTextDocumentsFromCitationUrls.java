package edu.isi.bmkeg.digitalLibrary.bin.bigMech;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.sql.ResultSet;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
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
import edu.isi.bmkeg.digitalLibrary.model.citations.ArticleCitation;
import edu.isi.bmkeg.ftd.model.FTD;
import edu.isi.bmkeg.ftd.model.qo.FTD_qo;
import edu.isi.bmkeg.lapdf.model.LapdfDocument;
import edu.isi.bmkeg.lapdf.xml.model.LapdftextXMLDocument;
import edu.isi.bmkeg.utils.Converters;
import edu.isi.bmkeg.utils.xml.XmlBindingTools;
import edu.isi.bmkeg.vpdmf.model.instances.LightViewInstance;

public class ImportFullTextDocumentsFromCitationUrls {

	public static class Options {

		@Option(name = "-corpus", usage = "Corpus", required = false, metaVar = "CORPUS")
		public String corpus = "";
		
		@Option(name = "-l", usage = "Database login", required = true, metaVar = "LOGIN")
		public String login = "";

		@Option(name = "-p", usage = "Database password", required = true, metaVar = "PASSWD")
		public String password = "";

		@Option(name = "-db", usage = "Database name", required = true, metaVar = "DBNAME")
		public String dbName = "";

		@Option(name = "-wd", usage = "Working directory", required = true, metaVar = "WDIR")
		public String workingDirectory = "";

	}

	private static Logger logger = Logger.getLogger(ImportFullTextDocumentsFromCitationUrls.class);

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		Options options = new Options();
		Pattern patt = Pattern.compile("PMC(\\d+)\\.");
		Pattern ftpPatt = Pattern.compile("ftp://(\\S+?)/");
		
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
			String selectSql = "SELECT * ";

			String countSql = "SELECT COUNT(*) ";
			
			String fromWhereSql = "FROM LiteratureCitation AS l LEFT JOIN (FTD as f) ON (l.fullText_id = f.vpdmfId), " +
					" ArticleCitation AS a, " +
					" Journal AS j, " +
					" URL AS u " + 
					"WHERE " + 
					" l.vpdmfId=a.vpdmfId AND " +
					" j.vpdmfId=a.journal_id AND " +
					" l.vpdmfId=u.resource_id AND "+ 
					" f.vpdmfId IS NULL; ";

			fromWhereSql += " ORDER BY l.vpdmfId";
			
			if( options.corpus != null && options.corpus.length() > 0 ){
				
				fromWhereSql = "FROM LiteratureCitation AS l LEFT JOIN (FTD as f) ON (l.fullText_id = f.vpdmfId), "
						+ "ArticleCitation AS a, "
						+ "Journal AS j, "
						+ "URL AS u,   "
						+ "Corpus_corpora__resources_LiteratureCitation AS link, "
						+ "Corpus AS c   "
						+ "WHERE l.vpdmfId=a.vpdmfId AND  "
						+ "j.vpdmfId=a.journal_id AND  "
						+ "l.vpdmfId=u.resource_id AND "
						+ "link.resources_id=l.vpdmfId AND "
						+ "link.corpora_id=c.vpdmfId AND "
						+ "c.name = '"+ options.corpus+ "' AND "
						+ "f.vpdmfId IS NULL; ";
			}

			de.getDigLibDao().getCoreDao().getCe().connectToDB();
			
			ResultSet countRs = de.getDigLibDao().getCoreDao().getCe().executeRawSqlQuery(
					countSql + fromWhereSql);
			countRs.next();
			int count = countRs.getInt(1);
						
			countRs.close();

			logger.info("Number of articles with no FTD: " + count);
			
			ResultSet rs = de.getDigLibDao().getCoreDao().getCe().executeRawSqlQuery(
					selectSql + fromWhereSql);

			Map<Long,Map<String,String>> lookup = new HashMap<Long,Map<String,String>>();
			
			while( rs.next() ) {

				Long vpdmfId = rs.getLong("l.vpdmfId");
				int pmid = rs.getInt("a.pmid");
				int pmcid = rs.getInt("a.pmcid");
				String url = rs.getString("u.url");
				String stem = "/pdfs/" + rs.getString("j.abbr").replaceAll("\\s+", "_") + "/" +
						rs.getInt("l.pubYear") + "/" +
						rs.getString("a.volume") + "/";

				if( pmcid == 0 ) {
					Matcher m = patt.matcher(url);
					if(m.find()) {
						String pmcidStr = m.group(1);
						pmcid = new Integer(pmcidStr).intValue();
					} else {
						throw new Exception("Can't find PMCID value");
					}
				}
				
				Map<String,String> hash = new HashMap<String,String>();
				lookup.put(vpdmfId, hash);
				hash.put("pmid", pmid + "");
				hash.put("pmcid", pmcid + "");
				hash.put("url", url);
				hash.put("stem", stem);

			}
			rs.close();
			
			//
			// Now go get the files...
			//
			List<Long> sortedVpdmfIds = new ArrayList<Long>(lookup.keySet());
			Collections.sort(sortedVpdmfIds);

			for(Long vpdmfId : sortedVpdmfIds) {
			
				total++;
				Map<String,String> hash = lookup.get(vpdmfId);
				String urlStr = hash.get("url");
				URL url = new URL(urlStr);
				String stem = hash.get("stem");
				String pmid = hash.get("pmid");
				String pmcid = hash.get("pmcid");
				File dir = new File(options.workingDirectory + stem );
				File pdf = new File(options.workingDirectory + stem + pmid + ".pdf" );
				
				if( !dir.exists() )
					dir.mkdirs();
				
				if( !pdf.exists() ) {
								
					URLConnection urlc = url.openConnection();
					InputStream inputStream = urlc.getInputStream();				
					OutputStream outputStream = new FileOutputStream(pdf);
				
					int read = 0;
					byte[] bytes = new byte[1024];
					while ((read = inputStream.read(bytes)) != -1) {
						outputStream.write(bytes, 0, read);
					}
				
				}
				
				String checksum = Converters.checksum(pdf);
				FTD_qo ftdQo = new FTD_qo();
				ftdQo.setChecksum(checksum);
				List<LightViewInstance> lviList = de.getExtDigLibDao().getCoreDao().list(ftdQo,
						"ArticleDocument");
				if (lviList.size() == 1) {
					continue;
				}
				
				// Being nice to NCBI
				Thread.sleep(1000);
				
				String eFetchUrl = "http://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pmc&id=" + pmcid;
				File pmcXml = new File(options.workingDirectory + stem + pmid + "_pmc.xml" );
				
				if( !pmcXml.exists() ) {
					url = new URL(eFetchUrl);
					URLConnection urlc = null;
					while( urlc == null ) {	
						
						try {
							
							urlc = url.openConnection();							
						
						} catch( IOException e ) {
							e.printStackTrace();
							Date d = new Date();
							Format formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
							String s = formatter.format(d);
							System.err.println(s);
							
							// Here, we wait 1 minute before trying again.
							Thread.sleep(60000);
							
						}
					}
					InputStream inputStream = urlc.getInputStream();				
					OutputStream outputStream = new FileOutputStream(pmcXml);

					int read = 0;
					byte[] bytes = new byte[1024];
					while ((read = inputStream.read(bytes)) != -1) {
						outputStream.write(bytes, 0, read);
					}
				}
				
				// Being nice to NCBI
				Thread.sleep(1000);

				//
				// Now add this as a FTD to the database. 
				// We are assuming that this is a simple insert
				//
				FTD ftd = new FTD();
				LapdfDocument doc = null;
				
				try {
					
					doc = de.blockifyFile(pdf);
					ftd.setPdfLoaded(pdf.exists());
				
				} catch(Exception e) {

					e.printStackTrace();
					nLapdfErrors++;
					System.out.println("lapdf errors = " + nLapdfErrors + " / " + total);
				
				}

				try {
					
					de.getExtDigLibDao().addSwfToFtd(pdf, ftd);
				
				} catch (Exception e) {
					
					e.printStackTrace();
					nSwfErrors++;
					System.out.println("swf errors = " + nSwfErrors + " / " + total);
					
				}
				
				File swfFile = new File(options.workingDirectory + stem + pmid + ".swf" );
				ftd.setSwfLoaded(swfFile.exists());
				
				ftd.setChecksum(checksum);
				ftd.setName(stem + pmid + ".pdf");
				
				File xmlFile = new File( options.workingDirectory + stem + pmid + "_lapdf.xml" );
				
				try {
				
					LapdftextXMLDocument xml = doc.convertToLapdftextXmlFormat();
					FileWriter writer = new FileWriter(xmlFile);
					XmlBindingTools.generateXML(xml, writer);

				} catch (Exception e) {
					
					e.printStackTrace();
					System.out.println("lapdf xml rendering error");
					
				}
				
				ftd.setXmlFile( stem + pmid + "_lapdf.xml");				
				ftd.setXmlLoaded(xmlFile.exists());
				
				ftd.setPmcXmlFile(stem + pmid + "_pmc.xml");
				ftd.setPmcLoaded(pmcXml.exists());

				ArticleCitation ac = de.getExtDigLibDao().getCoreDao().findById(
						vpdmfId, new ArticleCitation(), "ArticleCitation");
				ftd.setCitation(ac);
				ac.setPmcid(new Integer(pmcid));
				ac.setFullText(ftd);
				
				ftdQo = new FTD_qo();
				ftdQo.setChecksum(ftd.getChecksum());
				lviList = de.getExtDigLibDao().getCoreDao().list(ftdQo,
						"ArticleDocument");

				long adId = -1;
				if (lviList.size() == 0) {
					adId = de.getExtDigLibDao().getCoreDao().insert(ftd, "ArticleDocument");
				} else if (lviList.size() == 1) {
					ftd.setVpdmfId(lviList.get(0).getVpdmfId());
					adId = de.getExtDigLibDao().getCoreDao().update(ftd, "ArticleDocument");
				} else {
					throw new Exception("Ambiguous data");
				}
				
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
		
		logger.info("Number of swf conversion errors = " + nSwfErrors);
		logger.info("Number of lapdf conversion errors = " + nLapdfErrors);
		
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
