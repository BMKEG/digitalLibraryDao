package edu.isi.bmkeg.digitalLibrary.bin;

import java.io.File;

import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.isi.bmkeg.digitalLibrary.controller.DigitalLibraryEngine;
import edu.isi.bmkeg.digitalLibrary.utils.TerminologyExcelEngine;
import edu.isi.bmkeg.terminology.model.Ontology;
import edu.isi.bmkeg.terminology.model.Term;
import edu.isi.bmkeg.vpdmf.dao.CoreDao;

public class UploadTerminology {

	private static Logger logger = Logger.getLogger(UploadTerminology.class);

	public static class Options {

		@Option(name = "-xl", usage = "Excel terminology File", required = true, metaVar = "EXCEL-FILE")
		public File excelFile;

		@Option(name = "-l", usage = "Database login", required = true, metaVar = "LOGIN")
		public String login = "";

		@Option(name = "-p", usage = "Database password", required = true, metaVar = "PASSWD")
		public String password = "";

		@Option(name = "-db", usage = "Database name", required = true, metaVar  = "DBNAME")
		public String dbName = "";

		@Option(name = "-wd", usage = "Working directory", required = true, metaVar  = "WDIR")
		public String workingDirectory = "";
		
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		Options options = new Options();
		
		CmdLineParser parser = new CmdLineParser(options);
		CoreDao coreDao = null;

		try {
			
			parser.parseArgument(args);
		
			DigitalLibraryEngine de = null;
			
			de = new DigitalLibraryEngine();
			de.initializeVpdmfDao(options.login, options.password, options.dbName, options.workingDirectory);
			TerminologyExcelEngine xlEngine = new TerminologyExcelEngine();
			xlEngine.readFile(options.excelFile);
			Ontology ont = xlEngine.createOntologyFromExcel();

			coreDao = de.getDigLibDao().getCoreDao();
			coreDao.connectToDb();
			coreDao.insertInTrans(ont, "Ontology");
			
			for(Term t:ont.getTerm()) {
				coreDao.insertInTrans(t, "Term");				
			}
			
			coreDao.getCe().commitTransaction();
						
		} catch (CmdLineException e) {

			System.err.println(e.getMessage());
			System.err.print("Arguments: ");
			parser.printSingleLineUsage(System.err);
			System.err.println("\n\n Options: \n");
			parser.printUsage(System.err);
			System.exit(-1);
		
		} finally {
			
			coreDao.getCe().closeDbConnection();
		
		}
		

	}

}
