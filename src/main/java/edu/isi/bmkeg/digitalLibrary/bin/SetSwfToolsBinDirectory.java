package edu.isi.bmkeg.digitalLibrary.bin;

import java.io.File;

import org.apache.log4j.Logger;

import edu.isi.bmkeg.utils.Converters;

public class SetSwfToolsBinDirectory {

	public static String USAGE = "arguments: <directory-to-swftools-bin-directory>"; 

	private static Logger logger = Logger.getLogger(SetSwfToolsBinDirectory.class);
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		if( args.length != 1) {
			System.err.println(USAGE);
			System.exit(-1);
		}

		File dir = new File(args[0]);

		if( !dir.exists() ) {
			System.err.println(USAGE);
			System.exit(-1);
		}
			
		Converters.writeAppDirectory("swftools", dir);
		
	}

}
