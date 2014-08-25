package edu.isi.bmkeg.digitalLibrary.utils.elsevier;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

import org.apache.commons.io.FileUtils;
import org.drools.core.util.StringUtils;
import org.xml.sax.helpers.DefaultHandler;

public class ElsevierApiKey {

	public static String sciDirectStem = "";
	
	public static String readApiKey(String wd) throws IOException {

		File keyFile = new File(wd + "/elsevierKeyFile.txt" );
		
		if( keyFile.exists() ) 
			return FileUtils.readFileToString(keyFile);
		else 
			return null;

	}

}
