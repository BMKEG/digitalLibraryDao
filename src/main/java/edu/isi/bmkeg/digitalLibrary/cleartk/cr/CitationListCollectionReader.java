package edu.isi.bmkeg.digitalLibrary.cleartk.cr;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

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
public class CitationListCollectionReader extends DigitalLibraryCollectionReader {
	
	private static Logger logger = Logger.getLogger(CitationListCollectionReader.class);
		
	public static final String CITATIONS = ConfigurationParameterFactory
			.createConfigurationParameterName(CitationListCollectionReader.class,
					"citations");
	@ConfigurationParameter(mandatory = true, description = "The Digital Library URL")
	protected List<Long> citations = new ArrayList<Long>();

	// Query based on a query constructed with SqlQueryBuilder based on the TriagedArticle view.
	private static String sql = 
			"SELECT DISTINCT d.name, " +
			" l.title, " +
			" l.abstractText, " +
			" d.vpdmfId, " + 
			" d.citation_id " + 
			" FROM LiteratureCitation AS l " +
			"left join FTD AS d on (l.fullText_id = d.vpdmfId) " +
			" WHERE l.vpdmfId=";
	
	@Override
	public void initialize(UimaContext context) throws ResourceInitializationException {

		try {

			digLibEngine = new DigitalLibraryEngine();
			digLibEngine.initializeVpdmfDao(login, password, dbUrl, workingDirectory);	
			
			digLibEngine.getDigLibDao().getCoreDao().getCe().connectToDB();
			
			this.count = this.citations.size();

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
	 * sets the next docTxtHolder.
	 * 
	 * If skipUnknowns is true it will skips the citations whose aggregated code is "unknown"
	 * @throws TransformerException 
	 * @throws IOException 
	 */
	private void moveNext() throws Exception {
	
		String sqlToCall = sql + this.citations.get(pos);
		this.rs = digLibEngine.getDigLibDao().getCoreDao().getCe().executeRawSqlQuery( sqlToCall );
		
		// This code needs to have this.rs correctly set 
		// before running it. 
		this.readTextAndFtdFromRsNext();
		
	}

}
