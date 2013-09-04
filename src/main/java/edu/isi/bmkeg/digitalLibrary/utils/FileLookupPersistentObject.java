package edu.isi.bmkeg.digitalLibrary.utils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import edu.isi.bmkeg.utils.Converters;
import edu.isi.bmkeg.vpdmf.model.definitions.VPDMf;

public class FileLookupPersistentObject {

	private static Logger logger = Logger.getLogger(FileLookupPersistentObject.class);

	private Map<String, File> fLookup = new HashMap<String, File>();
	public static String FLOOKUP_FILE = "fileLookup.jObj";
	private File fLookupFile;
	
	private VPDMf top;
	private ClassLoader cl;
	
	public FileLookupPersistentObject() { 
		
		String dirPath = System.getProperty("user.dir");
		File dir = new File(dirPath);
		this.fLookupFile = new File( dir + "/" + FLOOKUP_FILE );
		
	}
	
	public void saveFileLookupFile() throws Exception {

		//
		// Dump whole UML model as serialized object to file
	    //
		InputStream is = new ByteArrayInputStream(
	            Converters.objectToByteArray(fLookup)
	            );
		FileOutputStream fos = new FileOutputStream(fLookupFile);
		byte[] buf = new byte[1024];
	    int len;
	    while ((len = is.read(buf)) > 0) {
	        fos.write(buf, 0, len);
	    }
	    fos.close();
		
	}

	public void loadFLookup() throws Exception {

		if( !fLookupFile.exists() ) {
			
			this.fLookup = new HashMap<String, File>();
			
		} else {
			
			byte[] b = Converters.fileContentsToBytesArray(fLookupFile);
			Object o = Converters.byteArrayToObject(b);
			this.fLookup = (Map<String, File>) o;
		
		}
		
	}

	public void clearFLookup() throws Exception {

		fLookupFile.delete();
		
	}

	
	public Map<String, File> getfLookup() {
		return fLookup;
	}

	public void setfLookup(Map<String, File> fLookup) {
		this.fLookup = fLookup;
	}
	
}
