package edu.isi.bmkeg.digitalLibrary.cleartk.cr;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.xml.transform.TransformerException;

import org.apache.log4j.Logger;
import org.apache.uima.UimaContext;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Progress;
import org.apache.uima.util.ProgressImpl;
import org.uimafit.component.JCasCollectionReader_ImplBase;
import org.uimafit.descriptor.ConfigurationParameter;
import org.uimafit.factory.ConfigurationParameterFactory;

import edu.isi.bmkeg.digitalLibrary.controller.DigitalLibraryEngine;
import edu.isi.bmkeg.digitalLibrary.model.citations.LiteratureCitation;
import edu.isi.bmkeg.ftd.model.FTD;

/**
 * We want to optimize this interaction for speed, so we run a
 * manual query over the underlying database involving a minimal subset of
 * tables.
 * 
 * @author burns
 * 
 */
public class DigitalLibraryCollectionReader extends JCasCollectionReader_ImplBase {
	
	protected static class DocumentTextHolder {
		public FTD ftd;
		public String text;
		
		public DocumentTextHolder(FTD ftd, String text) {
			this.ftd = ftd;
			this.text = text;
		}
	}
	
	private static Logger logger = Logger.getLogger(DigitalLibraryCollectionReader.class);

	public static final String CORPUS_NAME = ConfigurationParameterFactory
			.createConfigurationParameterName(DigitalLibraryCollectionReader.class,
					"corpusName");
	@ConfigurationParameter(mandatory = false, 
			description = "If specified, texts will be restricted to the given corpus")
	protected String corpusName;

	public static final String LOGIN = ConfigurationParameterFactory
			.createConfigurationParameterName(DigitalLibraryCollectionReader.class,
					"login");
	@ConfigurationParameter(mandatory = true, description = "Login for the Digital Library")
	protected String login;

	public static final String PASSWORD = ConfigurationParameterFactory
			.createConfigurationParameterName(DigitalLibraryCollectionReader.class,
					"password");
	@ConfigurationParameter(mandatory = true, description = "Password for the Digital Library")
	protected String password;

	public static final String WORKING_DIRECTORY = ConfigurationParameterFactory
			.createConfigurationParameterName(DigitalLibraryCollectionReader.class,
					"workingDirectory");
	@ConfigurationParameter(mandatory = true, description = "Working Directory for the Digital Library")
	protected String workingDirectory;
	
	public static final String DB_URL = ConfigurationParameterFactory
			.createConfigurationParameterName(DigitalLibraryCollectionReader.class,
					"dbUrl");
	@ConfigurationParameter(mandatory = true, description = "The Digital Library URL")
	protected String dbUrl;
		
	protected ResultSet rs;

	protected boolean eof = false;
		
	protected long startTime, endTime;

	protected int pos = 0, count = 0;

	protected DigitalLibraryEngine digLibEngine;

	protected DocumentTextHolder docTxtHolder;

	@Override
	public void initialize(UimaContext context) throws ResourceInitializationException {

		try {

			digLibEngine = new DigitalLibraryEngine();
			digLibEngine.initializeVpdmfDao(login, password, dbUrl, workingDirectory);	
			
			// Query based on a query constructed with SqlQueryBuilder based on the TriagedArticle view.
			String selectSql = "SELECT DISTINCT d.name, " + 
					" l.title, " +
					" l.abstractText, " +
					" d.vpdmfId, " + 
					" d.name, " + 
					" l.vpdmfId ";

			String countSql = "SELECT COUNT(*) ";
			
			String fromWhereSql = "FROM LiteratureCitation AS l " +
					"inner join FTD AS d on (l.fullText_id = d.vpdmfId), " + 
					" Corpus AS c, " +
					" Corpus_corpora__resources_LiteratureCitation AS link " + 
					"WHERE " + 
					" c.name = '" + corpusName +  "' AND " +
					" l.vpdmfId=link.resources_id AND " +
					" c.vpdmfId=link.corpora_id";

			if( corpusName == null ) {
				fromWhereSql = "FROM LiteratureCitation AS l " +
						"inner join FTD AS d on (l.fullText_id = d.vpdmfId)";
			}
			
			digLibEngine.getDigLibDao().getCoreDao().getCe().connectToDB();
			
			ResultSet countRs = digLibEngine.getDigLibDao().getCoreDao().getCe().executeRawSqlQuery(
					countSql + fromWhereSql);
			countRs.next();
			this.count = countRs.getInt(1);
			countRs.close();

			this.rs = digLibEngine.getDigLibDao().getCoreDao().getCe().executeRawSqlQuery(
					selectSql + fromWhereSql);
			
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
	private void moveNext() throws SQLException, IOException, TransformerException {
		
		docTxtHolder = extractTextFromFtdXml();
		
	}
	
	/** 
	 * Computes the next AggregatedScore and advances the rs cursor
	 * 
	 * @return the next AggregatedScore or null if EOF. 
	 *
	 * This function expects a state in which either
 	 * a) rs already contains a valid record and this record corresponds to the first time 
	 * the current vpdmfId is seen or
	 * b) eof is true
	 * @throws SQLException 
	 * @throws IOException 
	 * @throws TransformerException 
	 * 
	 */
	protected DocumentTextHolder extractTextFromFtdXml() throws 
			SQLException, IOException, TransformerException,FileNotFoundException  {

		eof = !rs.next();
		
		if (eof)
			return null;
		
		Long ftdId = rs.getLong("d.vpdmfId");
		String pdfPath = rs.getString("d.name");
		String title = rs.getString("l.title");
		String abst = rs.getString("l.abstractText");
		
		FTD ftd = new FTD();
		ftd.setVpdmfId(ftdId);
		ftd.setName(pdfPath);
		LiteratureCitation lc = new LiteratureCitation();
		ftd.setCitation(lc);
		lc.setTitle(title);
		lc.setAbstractText(abst);
		
		String plainText = this.digLibEngine.getExtDigLibDao().retrieveTextFromFtd(ftd);			
		
		return new DocumentTextHolder(ftd, plainText);
		
	}
	

		
	protected void error(String message) {
		logger.error(message);
	}

	@SuppressWarnings("unused")
	protected void warn(String message) {
		logger.warn(message);
	}

	@SuppressWarnings("unused")
	protected void debug(String message) {
		logger.error(message);
	}

	public Progress[] getProgress() {		
		Progress progress = new ProgressImpl(
				this.pos, 
				this.count, 
				Progress.ENTITIES);
		
        return new Progress[] { progress };
	}

}
