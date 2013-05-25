package edu.isi.bmkeg.digitalLibrary.bin;

import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import edu.isi.bmkeg.digitalLibrary.controller.DigitalLibraryEngine;
import edu.isi.bmkeg.digitalLibrary.model.citations.Corpus;
import edu.isi.bmkeg.digitalLibrary.model.qo.citations.Corpus_qo;
import edu.isi.bmkeg.vpdmf.model.definitions.VPDMf;
import edu.isi.bmkeg.vpdmf.model.instances.LightViewInstance;

public class EditArticleCorpus {

	public static String USAGE = "Either adds or edits a uniquely named ArticleCorpus.\n" +
			"arguments: <corpus-name> <description> <owner-name> " + 
			"<dbName> <login> <password> "; 

	private static Logger logger = Logger.getLogger(EditArticleCorpus.class);

	private VPDMf top;
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		if (args.length != 6) {
			System.err.println(USAGE);
			System.exit(1);
		}
		
		String name = args[0];
		String description = args[1];
		String owner = args[2];
		String dbName = args[3];
		String login = args[4];
		String password = args[5];
		
		DigitalLibraryEngine de = null;
		
		de = new DigitalLibraryEngine();
		de.initializeVpdmfDao(login, password, dbName);

		Corpus_qo qc = new Corpus_qo();
		qc.setName(name);
		List<LightViewInstance> lviList = de.getDigLibDao().listArticleCorpus(qc);
		
		if( lviList.size() == 0 ) {

			Corpus c = new Corpus();
			
			c.setName(name);
			c.setDescription(description);
			c.setOwner(owner);
			Date d = new Date();
			c.setDate(d.toString());
			
			de.insertArticleCorpus(c);
		
		} else if( lviList.size() == 1 ) {
			
			LightViewInstance lvi = lviList.get(0);
			
			Corpus c = de.getDigLibDao().findArticleCorpusById( lvi.getVpdmfId() );
			
			c.setName(name);
			c.setDescription(description);
			c.setOwner(owner);
			
			de.getDigLibDao().updateArticleCorpus(c);
			
		}

	}

}
