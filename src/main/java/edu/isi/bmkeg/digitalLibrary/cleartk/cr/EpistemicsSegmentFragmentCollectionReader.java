package edu.isi.bmkeg.digitalLibrary.cleartk.cr;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.apache.log4j.Logger;
import org.apache.uima.UimaContext;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Progress;
import org.apache.uima.util.ProgressImpl;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.select.Elements;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.uimafit.component.JCasCollectionReader_ImplBase;
import org.uimafit.descriptor.ConfigurationParameter;
import org.uimafit.factory.ConfigurationParameterFactory;

import edu.isi.bmkeg.digitalLibrary.controller.DigitalLibraryEngine;
import edu.isi.bmkeg.digitalLibrary.model.qo.citations.Corpus_qo;

/**
 * We want to optimize this interaction for speed, so we run a
 * manual query over the underlying database involving a minimal subset of
 * tables.
 * 
 * @author burns
 * 
 */
public class EpistemicsSegmentFragmentCollectionReader extends DigitalLibraryCollectionReader {
	
	private static Logger logger = Logger.getLogger(EpistemicsSegmentFragmentCollectionReader.class);
			
	public static final String PMIDS = ConfigurationParameterFactory
			.createConfigurationParameterName(EpistemicsSegmentFragmentCollectionReader.class,
					"pmids");
	@ConfigurationParameter(mandatory = true, description = "A list of all the pmids")
	protected List<String> pmids = new ArrayList<String>();
	
	private static String countSql = 
			"SELECT COUNT(*) ";

	private static String selectSql = 
			"SELECT l.vpdmfId, a.pmid, f.name, frg.vpdmfId, frg.frgOrder, blk.vpdmfOrder, blk.text, blk.code ";
	
	private static String fromWhereSql = 
			"FROM LiteratureCitation AS l," +
			" ArticleCitation as a, FTD as f, " + 
			" FTDFragment as frg, FTDFragmentBlock as blk " +
			" WHERE " +
			"blk.fragment_id = frg.vpdmfId AND " +
			"l.fullText_id = f.vpdmfId AND " +
			"l.vpdmfId = a.vpdmfId AND " +
			"frg.ftd_id = f.vpdmfId AND " +
			"frg.frgType = 'epistSeg' ";
	
	private static String pmidSql = " AND a.pmid = ";

	private static String orderBySql = 
			" ORDER BY l.vpdmfId, frg.vpdmfId, frg.frgOrder, blk.vpdmfOrder;";
	
	@Override
	public void initialize(UimaContext context) throws ResourceInitializationException {

		try {

			digLibEngine = new DigitalLibraryEngine();
			digLibEngine.initializeVpdmfDao(login, password, dbUrl, workingDirectory);	
			
			digLibEngine.getDigLibDao().getCoreDao().getCe().connectToDB();
			
			this.count = this.pmids.size();

			this.pos = 0;
			this.startTime = System.currentTimeMillis();

			//
			// Calling moveNext() to compute the first currentAs
			//
			moveNext();
			
		} catch (Exception e) {

			throw new ResourceInitializationException(e);

		}

	}

	/**
	 * @see com.ibm.uima.collection.CollectionReader#getNext(com.ibm.uima.cas.CAS)
	 */
	public void getNext(JCas jcas) throws IOException, CollectionException {

		try {
			
			if( docTxtHolder == null )
			    jcas.setDocumentText("");
			else
				jcas.setDocumentText( docTxtHolder.text );

			edu.isi.bmkeg.ftd.uimaTypes.FTD doc = 
					new edu.isi.bmkeg.ftd.uimaTypes.FTD(jcas);

			doc.setVpdmfId(docTxtHolder.ftd.getVpdmfId());
			doc.setName(docTxtHolder.ftd.getName());
		    doc.addToIndexes(jcas);

		    moveNext();
		    
		    pos++;
		    if( (pos % 1000) == 0) {
		    	System.out.println("Processing " + pos + "th document.");
		    }
		    
		} catch (Exception e) {

			throw new CollectionException(e);

		}

	}

	/**
	 * @see com.ibm.uima.arg0collection.base_cpm.BaseCollectionReader#hasNext()
	 */
	public boolean hasNext() throws IOException, CollectionException {
		return docTxtHolder != null;
	}
		
	public void close() throws IOException {
		try {
			digLibEngine.getDigLibDao().getCoreDao().getCe().closeDbConnection();
		} catch (Exception e) {
			throw new IOException(e);
		}
	}
	
	/**
	 * @throws Exception 
	 */
	private void moveNext() throws Exception {
		
		Pattern p2 = Pattern.compile("^(\\D?)(\\d+)(\\D*)$");
	
		String sqlToCall = selectSql + fromWhereSql +
				pmidSql + "'" + this.pmids.get(pos) + "'" + orderBySql;
				;
		this.rs = digLibEngine.getDigLibDao().getCoreDao().getCe().executeRawSqlQuery( sqlToCall );
		
		Map<String,Map<String,String>> figHash = 
				new HashMap<String,Map<String,String>>();
		
		String frgText = "";
		String annText = "";
		int j = 0, pos = 0;
		
		while( rs.next() ) {

			Long citationId = rs.getLong("l.vpdmfId");
			String fileStem = rs.getString("f.name");
			int pmid = rs.getInt("a.pmid");
			String frgOrder = rs.getString("frg.frgOrder");
			String blkId = rs.getString("blk.vpdmfOrder");
			
			String blkCode = rs.getString("blk.code");
			if( blkCode == null || blkCode.equals("-") ) 
				continue;
			
			String blkText = rs.getString("blk.text");
			blkText = blkText.replaceAll("\\s+", " ");
			blkText = blkText.replaceAll("\\-\\s+", "");
						
			// Parse the frgOrder code.
			// First split any '+' codes
			// then enumerate numbers for figures and ignore Supplemental data. 
			String[] splitCodes = frgOrder.split("\\+");
			for( String s : splitCodes) {
				
				Matcher m = p2.matcher(s);
				if( m.find() ) {
					
					String suppCode = m.group(1);
					String number = m.group(2);
					String letters = m.group(3);
					
					if( suppCode.equals("S") )
						continue;

					for(int i=0; i<letters.length(); i++) {
						String l = letters.substring(i, i+1);
						
						if(  !figHash.containsKey(frgOrder) ) {
							frgText = "";
							annText = "";
						} else {
							Map<String, String> map = figHash.get(frgOrder);
							frgText = map.get("txt");
							annText = map.get("ann");
						}
						
						frgText += blkText;
						
						int start = frgText.indexOf(blkText);
						int end = start + blkText.length() - 1;
						
						annText += "T"+ (j++) + "\t" + 
								blkCode.replaceAll(": ", "_") + " " + 
								start + " " +
								end + "\t" + 
								blkText +"\n";

						Map<String, String> map = new HashMap<String, String>();
						figHash.put( number+l , map);
						map.put("txt", frgText);
						map.put("ann", annText);
						
						pos += pos + blkText.length();
						
					}
			
				} else {
					
					logger.info("skipping fragment coded: " + pmid + "_" + s);
										
				}
			
			}
							
		}
		rs.close();
		
		// This code needs to have this.rs correctly set 
		// before running it. 
		this.readTextAndFtdFromRsNext();
		
	}

}
