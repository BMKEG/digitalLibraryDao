package edu.isi.bmkeg.digitalLibrary.bin;

import java.io.File;
import java.net.URL;

import org.apache.log4j.Logger;

import edu.isi.bmkeg.vpdmf.bin.BuildDatabaseFromVpdmfArchive;
import edu.isi.bmkeg.vpdmf.model.definitions.VPDMf;

public class BuildDigitalLibraryDatabase {

	public static String USAGE = "arguments: <dbName> <login> <password>"; 

	private static Logger logger = Logger.getLogger(BuildDigitalLibraryDatabase.class);

	private VPDMf top;
	
	public static void main(String[] args) {

		if( args.length != 3 ) {
			System.err.println(USAGE);
			System.exit(-1);
		}
		
		try { 

			URL url = ClassLoader.getSystemClassLoader().getResource("edu/isi/bmkeg/digitalLibrary/digitalLibrary-mysql.zip");
			String buildFilePath = url.getFile();
			File buildFile = new File( buildFilePath );

			String[] newArgs = new String[] { 
					buildFile.getPath(), args[0], args[1], args[2] 
					};
			
			BuildDatabaseFromVpdmfArchive.main(newArgs);
						
			logger.info("Digital Library Database " + args[0] + " successfully created.");
				
		} catch (Exception e) {
			
			e.printStackTrace();
		
		}
		
	}

}
