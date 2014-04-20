package edu.isi.bmkeg.digitalLibrary.bin;

import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.isi.bmkeg.digitalLibrary.controller.DigitalLibraryEngine;
import edu.isi.bmkeg.digitalLibrary.model.citations.Corpus;
import edu.isi.bmkeg.digitalLibrary.model.qo.citations.Corpus_qo;
import edu.isi.bmkeg.vpdmf.model.instances.LightViewInstance;

public class EditArticleCorpus {

	public static class Options {

		@Option(name = "-name", usage = "Corpus name", required = true, metaVar = "NAME")
		public String name;
		
		@Option(name = "-desc", usage = "Corpus description", required = true, metaVar = "DESCRIPTION")
		public String description;
		
		@Option(name = "-owner", usage = "Corpus owner", required = true, metaVar = "OWNER")
		public String owner;
		
		@Option(name = "-regex", usage = "Regular expression to recognize incoming files", required = false, metaVar = "REGEX")
		public String regex;
		
		@Option(name = "-l", usage = "Database login", required = true, metaVar = "LOGIN")
		public String login = "";

		@Option(name = "-p", usage = "Database password", required = true, metaVar = "PASSWD")
		public String password = "";

		@Option(name = "-db", usage = "Database name", required = true, metaVar  = "DBNAME")
		public String dbName = "";

		@Option(name = "-wd", usage = "Working directory", required = true, metaVar  = "WDIR")
		public String workingDirectory = "";
		
	}

	private static Logger logger = Logger.getLogger(EditArticleCorpus.class);
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		Options options = new Options();
		
		CmdLineParser parser = new CmdLineParser(options);

		try {
			
			parser.parseArgument(args);
		
		} catch (CmdLineException e) {
			
			System.err.println(e.getMessage());
			System.err.print("Arguments: ");
			parser.printSingleLineUsage(System.err);
			System.err.println("\nEither adds or edits a uniquely named ArticleCorpus.");
			System.err.println("\n\n Options: \n");
			parser.printUsage(System.err);
			System.exit(-1);
		}
				
		DigitalLibraryEngine de = null;
		
		de = new DigitalLibraryEngine();
		de.initializeVpdmfDao(options.login, options.password, options.dbName, options.workingDirectory);

		Corpus_qo qc = new Corpus_qo();
		qc.setName(options.name);
		List<LightViewInstance> lviList = de.getDigLibDao().listArticleCorpus(qc);
		
		if( lviList.size() == 0 ) {

			Corpus c = new Corpus();
			
			c.setName(options.name);
			c.setDescription(options.description);
			c.setOwner(options.owner);
			
			c.setRegex(options.regex.replaceAll("\\\\", "__BACKSLASH__"));
			
			Date d = new Date();
			c.setDate(d.toString());
			
			de.getDigLibDao().insertArticleCorpus(c);
		
		} else if( lviList.size() == 1 ) {
			
			LightViewInstance lvi = lviList.get(0);
			
			Corpus c = de.getDigLibDao().findArticleCorpusById( lvi.getVpdmfId() );
			
			c.setName(options.name);
			c.setDescription(options.description);
			
			c.setRegex(options.regex.replaceAll("\\\\", "__BACKSLASH__"));
			
			c.setOwner(options.owner);
			
			Date d = new Date();
			c.setDate(d.toString());
			
			de.getDigLibDao().updateArticleCorpus(c);
			
		}

	}

}
