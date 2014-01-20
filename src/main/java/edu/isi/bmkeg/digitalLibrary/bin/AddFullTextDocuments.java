package edu.isi.bmkeg.digitalLibrary.bin;

import java.io.File;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import edu.isi.bmkeg.ftd.model.FTD;
import edu.isi.bmkeg.lapdf.controller.LapdfVpdmfEngine;
import edu.isi.bmkeg.lapdf.model.LapdfDocument;
import edu.isi.bmkeg.lapdf.xml.model.LapdftextXMLDocument;
import edu.isi.bmkeg.utils.Converters;
import edu.isi.bmkeg.utils.xml.XmlBindingTools;

public class AddFullTextDocuments
{

	private static Logger logger = Logger.getLogger(AddFullTextDocuments.class);
	
	private static String USAGE = "usage: <input-file/dir> <dbName> <login> <password> [<rule-file>]";

	public static void main(String args[]) throws Exception	{

		if (args.length < 4 || args.length > 5 ) {
			System.err.println(USAGE);
			System.exit(1);
		}
		
		String inputFileOrFolderPath = args[0];
		String dbName = args[1];
		String login = args[2];
		String password = args[3];
		String ruleFileLocation = null;
		
		if (args.length == 5) 
			ruleFileLocation = args[4];
 	
		LapdfVpdmfEngine lapdfEng = null;
		if (ruleFileLocation != null) {
			logger.info("Using rulefile " + ruleFileLocation);
			lapdfEng = new LapdfVpdmfEngine(new File(ruleFileLocation));
		} else {
			lapdfEng = new LapdfVpdmfEngine();
		}
		
		lapdfEng.initializeVpdmfDao(login, password, dbName);
		
		File fOrD = new File( inputFileOrFolderPath );
		if( !fOrD.exists() ) { 
			System.err.print( inputFileOrFolderPath + " does not exist.");
			System.exit(-1);
		}
	
		if( fOrD.isDirectory() ) {
			
			Pattern p = Pattern.compile("\\.pdf$");
			Map<String, File> m = Converters.recursivelyListFiles(
					fOrD, new HashMap<String, File>(), p
					);
			Iterator<File> fIt = m.values().iterator();
			while( fIt.hasNext() ) {
				File f = fIt.next();
				
				FTD ftd = new FTD();
			
				LapdfDocument doc = lapdfEng.blockifyFile(f);
				
				ftd.setChecksum( Converters.checksum(f) );
				ftd.setName( f.getPath() );
			
				LapdftextXMLDocument xml = doc.convertToLapdftextXmlFormat();
				StringWriter writer = new StringWriter();
				XmlBindingTools.generateXML(xml, writer);
				ftd.setXml( writer.toString() );
				
				lapdfEng.getFtdDao().getCoreDao().insert(ftd, "FullTextDocument");
						
			}
			
		} else {
		
			LapdfDocument doc = lapdfEng.blockifyFile(fOrD);
			
			FTD ftd = new FTD();

			ftd.setChecksum( Converters.checksum(fOrD) );
			ftd.setName( fOrD.getPath() );
		
			LapdftextXMLDocument xml = doc.convertToLapdftextXmlFormat();
			StringWriter writer = new StringWriter();
			XmlBindingTools.generateXML(xml, writer);
			ftd.setXml( writer.toString() );
			
			lapdfEng.getFtdDao().getCoreDao().insert(ftd, "FullTextDocument");
			
		}
	
	}

}
