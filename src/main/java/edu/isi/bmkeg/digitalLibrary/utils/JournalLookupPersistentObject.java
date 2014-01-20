package edu.isi.bmkeg.digitalLibrary.utils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import edu.isi.bmkeg.digitalLibrary.model.citations.Journal;
import edu.isi.bmkeg.utils.Converters;
import edu.isi.bmkeg.vpdmf.dao.CoreDaoImpl;
import edu.isi.bmkeg.vpdmf.model.definitions.VPDMf;
import edu.isi.bmkeg.vpdmf.model.definitions.ViewDefinition;
import edu.isi.bmkeg.vpdmf.model.instances.ViewBasedObjectGraph;
import edu.isi.bmkeg.vpdmf.model.instances.ViewInstance;

public class JournalLookupPersistentObject {

	private static Logger logger = Logger.getLogger(JournalLookupPersistentObject.class);

	private static String JLOOKUP_FILE = "journalAbbrLookup.jObj";
	
	private static Map<String, Journal> instance = null;
	
	public JournalLookupPersistentObject() { 
	}
	
	public static Map<String, Journal> regenerateJournalLookupFile(
			String dbName, String login, String password, File jLookupFile) 
					throws Exception {

		//
		// Build a temporary empty digital library database 
		// - this will throw an error if the database already exists.
		//
		URL archiveUrl = JournalLookupPersistentObject.class.getClassLoader().getResource(
				"edu/isi/bmkeg/digitalLibrary/digitalLibrary-mysql.zip");
		File archiveFile = new File( archiveUrl.getPath() );

		CoreDaoImpl dlVpdmf = new CoreDaoImpl();
		dlVpdmf.init(login, password, dbName);
			
		//
		// Query this database to generate a local map of journals indexed by name.
		//
		String viewName = "Journal";
		ViewBasedObjectGraph vbog = new ViewBasedObjectGraph(
				dlVpdmf.getTop(), dlVpdmf.getCl(), 
				viewName);
		ViewDefinition vd = dlVpdmf.getTop().getViews().get(viewName);
		ViewInstance qVi = new ViewInstance(vd);
		
		Map<String,Journal> jLookup = new HashMap<String, Journal>();

		dlVpdmf.getCe().connectToDB();
		
		int jCount = dlVpdmf.getCe().executeCountQuery(qVi);
		int pageSize = 1000;
		dlVpdmf.getCe().setMaxReturnedInQuery(pageSize*100);
		for( int i=0; i<jCount+pageSize; i=i+pageSize) {
			
			qVi = new ViewInstance(vd);
			List<ViewInstance> viewList = dlVpdmf.getCe().executeFullQuery(qVi, true, i, pageSize);
			
			Iterator<ViewInstance> iterator = viewList.iterator();
			while( iterator.hasNext() ) {
				ViewInstance viewInstance = (ViewInstance) iterator.next();
				vbog.viewToObjectGraph(viewInstance);
				Journal j = (Journal) vbog.readPrimaryObject();
				jLookup.put(j.getAbbr(), j);
			}
	
		}
		
		dlVpdmf.getCe().closeDbConnection();	
				
		//
		// Dump whole journal lookup table as serialized object to file
	    //
		InputStream is = new ByteArrayInputStream(
	            Converters.objectToByteArray(jLookup)
	            );
		FileOutputStream fos = new FileOutputStream(jLookupFile);
		byte[] buf = new byte[1024];
	    int len;
	    while ((len = is.read(buf)) > 0) {
	        fos.write(buf, 0, len);
	    }
	    fos.close();
	    	    
	    return jLookup;
		
	}

	public static Map<String, Journal> readJLookup(String dbName, String login, String password) throws Exception {

		if( instance != null )
			return instance;
		
		String dirPath = System.getProperty("user.dir");
		File dir = new File(dirPath);
		File jLookupFile = new File( dir + "/" + JLOOKUP_FILE );
		Map<String, Journal> jLookup = new HashMap<String, Journal>();
		
		if( !jLookupFile.exists() ) {

			logger.info("No journal lookup file found, regenerating the file.");
			jLookup = regenerateJournalLookupFile(dbName, login, password, jLookupFile);

		} else {

			logger.info("Journal lookup file found, loading directly from disk");
			byte[] b = Converters.fileContentsToBytesArray(jLookupFile);
			Object o = Converters.byteArrayToObject(b);
			jLookup = (Map<String, Journal>) o;
		
		}
		
		instance = jLookup;
		
		return jLookup;
	}

	public static Map<String, Journal> readJLookup() throws Exception {

		String dirPath = System.getProperty("user.dir");
		File dir = new File(dirPath);
		File jLookupFile = new File( dir + "/" + JLOOKUP_FILE );
		Map<String, Journal> jLookup = null;
		
		if( !jLookupFile.exists() ) {

			throw new Exception("No journal lookup file found, need to regenerate the file.");

		} else {

			logger.info("Journal lookup file found, loading directly from disk");
			byte[] b = Converters.fileContentsToBytesArray(jLookupFile);
			Object o = Converters.byteArrayToObject(b);
			jLookup = (Map<String, Journal>) o;
		
		}
		
		return jLookup;
	}
	
}
