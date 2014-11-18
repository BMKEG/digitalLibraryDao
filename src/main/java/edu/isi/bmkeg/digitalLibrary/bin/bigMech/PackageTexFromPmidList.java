package edu.isi.bmkeg.digitalLibrary.bin.bigMech;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.select.Elements;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import com.google.common.io.Files;

import edu.isi.bmkeg.digitalLibrary.controller.DigitalLibraryEngine;
import edu.isi.bmkeg.digitalLibrary.model.citations.ArticleCitation;
import edu.isi.bmkeg.digitalLibrary.model.citations.Corpus;
import edu.isi.bmkeg.digitalLibrary.model.qo.citations.Corpus_qo;
import edu.isi.bmkeg.utils.Converters;
import edu.isi.bmkeg.vpdmf.dao.CoreDao;
import edu.isi.bmkeg.vpdmf.model.instances.LightViewInstance;


/**
 * This command-line utility loads a file of OA PMC identifiers, loads their citations, 
 * downloads PDFs and XML data for each file
 * 
 * @author burns
 *
 */
public class PackageTexFromPmidList {

	public Connection dbConnection;
	protected Set<String> drivers = new HashSet<String>();
	protected Statement stat;
	protected Statement uStat;
	
	public static class Options {

		@Option(name = "-zipFile", usage = "Zip File", required = true, metaVar = "ZIP")
		public File zipFile;

		@Option(name = "-pmidListFile", usage = "list of pmid values", required = true, metaVar = "PMIDS")
		public File pmidListFile;

		@Option(name = "-l", usage = "Database login", required = true, metaVar = "LOGIN")
		public String login = "";

		@Option(name = "-p", usage = "Database password", required = true, metaVar = "PASSWD")
		public String password = "";

		@Option(name = "-db", usage = "Database name", required = true, metaVar = "DBNAME")
		public String dbName = "";

		@Option(name = "-wd", usage = "Working directory", required = true, metaVar = "WDIR")
		public String workingDirectory = "";

	}

	private static Logger logger = Logger.getLogger(PackageTexFromPmidList.class);

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		Options options = new Options();
		
		CmdLineParser parser = new CmdLineParser(options);

		try {

			parser.parseArgument(args);
			
			Set<Integer> pmidSet = new HashSet<Integer>();
			
			FileReader fr = new FileReader(options.pmidListFile);
			BufferedReader br = new BufferedReader(fr);
			String  thisLine = null;
	        while ((thisLine = br.readLine()) != null) {
				Integer pmid = new Integer(thisLine);
				pmidSet.add(pmid);
	        }       
			List<Integer> pmids = new ArrayList<Integer>(pmidSet);
	        
			DigitalLibraryEngine de = new DigitalLibraryEngine();
			de.initializeVpdmfDao(options.login, options.password, options.dbName, options.workingDirectory);

			CoreDao coreDao = de.getDigLibDao().getCoreDao();
			de.getDigLibDao().getCoreDao().connectToDb();		
										
			String wd = coreDao.getWorkingDirectory();

			// Create a buffer for reading the files
			byte[] buf = new byte[1024];

			// Create the ZIP output stream for a binary file 
			// (don't want to load everything into memory)
			ZipOutputStream out = new ZipOutputStream(new FileOutputStream(
					options.zipFile));
			
			for(Integer pmid : pmids) {
			
				String sql = "select lc.title, lc.abstractText, ftd.name " + 
					"from " + 
					"FTD as ftd, " +
					"LiteratureCitation as lc, " +				
					"ArticleCitation as ac " +
					"where " +
					"lc.vpdmfId = ac.vpdmfId AND " + 
					"lc.fullText_id = ftd.vpdmfId AND " +
					"ac.pmid = " + pmid;

				ResultSet rs = coreDao.getCe().executeRawSqlQuery(sql);
				
				while( rs.next() ) {
					String title = rs.getString("lc.title");
					String abst = rs.getString("lc.abstractText");
					String pdfPath = rs.getString("ftd.name");
					String stemPath = wd + "/" + pdfPath.substring(0, pdfPath.lastIndexOf("."));

					File xml = new File(stemPath + "_pmc.xml");					
					if( xml.exists() ) {
						
						FileInputStream in = new FileInputStream(xml);

						// Add ZIP entry to output stream.
						out.putNextEntry(new ZipEntry(xml.getName()));

						// Transfer bytes from the file to the ZIP file
						int len;
						while ((len = in.read(buf)) > 0) {
							out.write(buf, 0, len);
						}

						// Complete the entry
						out.closeEntry();
						in.close();
						
					} else {
						
						continue;
					
					}
				
					File txt = new File(stemPath + ".txt");					
					if( !txt.exists() ) {
						
						FileReader inputReader = new FileReader(xml);
						StringWriter outputWriter = new StringWriter();

						TransformerFactory tf = TransformerFactory.newInstance();

						// stylesheet
						Resource xslResource = new ClassPathResource(
								"jatsPreviewStyleSheets/xslt/main/jats-html-textOnly.xsl");
						StreamSource xslt = new StreamSource(xslResource.getInputStream());
						Transformer transformer = tf.newTransformer(xslt);

						StreamSource source = new StreamSource(inputReader);
						StreamResult result = new StreamResult(outputWriter);
						transformer.transform(source, result);
						String html = outputWriter.toString();

						String htmlPath = pdfPath.substring(0, pdfPath.lastIndexOf(".pdf"))
								+ ".html";
						FileUtils.writeStringToFile(new File(wd + "/" + htmlPath), html);

						Document doc = Jsoup.parse(html);

						String plainText = title + "\n" + abst + "\n";

						Elements bodyEls = doc.select("div");
						for (Element bodyEl : bodyEls) {
							for (Node n : bodyEl.select("tr")) {
								n.remove();
							}
							for (Node n : bodyEl.getElementsByClass("object-id")) {
								n.remove();
							}
							for (Node n : bodyEl.select("a")) {
								addFormattingSuffixes((Element) n, "A");
							}
							for (Node n : bodyEl.select("i")) {
								addFormattingSuffixes((Element) n, "I");
							}
							for (Node n : bodyEl.select("b")) {
								addFormattingSuffixes((Element) n, "B");
							}
							for (Node n : bodyEl.select("sup")) {
								addFormattingSuffixes((Element) n, "SUP");
							}
							for (Node n : bodyEl.select("sub")) {
								addFormattingSuffixes((Element) n, "SUB");
							}
							for (Node n : bodyEl.select("h1")) {
								addFormattingSuffixes((Element) n, "H");
							}
							for (Node n : bodyEl.select("h2")) {
								addFormattingSuffixes((Element) n, "H");
							}
							for (Node n : bodyEl.select("h3")) {
								addFormattingSuffixes((Element) n, "H");
							}
							for (Node n : bodyEl.select("h4")) {
								addFormattingSuffixes((Element) n, "H");
							}
							for (Node n : bodyEl.select("p")) {
								addFormattingSuffixes((Element) n, "P");
							}
						}

						String t = doc.text();
						t = t.replaceAll("__s_H__", "\n");
						t = t.replaceAll("__s_P__", "");

						// put a period after headings for sentence detection
						t = t.replaceAll("__e_H__", ".\n");
						t = t.replaceAll("\\.\\s*\\.", ".");
						t = t.replaceAll("__e_P__", "\n") + "\n";

						plainText += t;

						FileUtils.writeStringToFile(txt, plainText);
						
					}
						
					FileInputStream in = new FileInputStream(txt);

					// Add ZIP entry to output stream.
					out.putNextEntry(new ZipEntry(txt.getName()));

					// Transfer bytes from the file to the ZIP file
					int len;
					while ((len = in.read(buf)) > 0) {
						out.write(buf, 0, len);
					}

					// Complete the entry
					out.closeEntry();
					in.close();
					
				}
				
			}

			// Complete the ZIP file
			out.close();			
			
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
	
	private static Element addFormattingSuffixes(Element el, String suffix) {
		String t = el.text();;
		String s = " __s_" + suffix + "__ ";
		String e = " __e_" + suffix + "__ ";
		
		if( t.indexOf("__s_") == -1  && t.length() > 0) {
			el.text( s + t + e );
		} 
		
		return el;
	}
	
}
