package edu.isi.bmkeg.digitalLibrary.utils.pubmed;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import edu.isi.bmkeg.digitalLibrary.model.citations.Journal;
import edu.isi.bmkeg.vpdmf.controller.VPDMfKnowledgeBaseBuilder;
import edu.isi.bmkeg.vpdmf.model.definitions.VPDMf;
import edu.isi.bmkeg.vpdmf.model.instances.ViewBasedObjectGraph;
import edu.isi.bmkeg.vpdmf.model.instances.ViewInstance;

/**
 * Downloads the latest list of journals from ftp://ftp.ncbi.nih.gov/pubmed/J_Entrez.txt 
 * and populates the base Journal lookup structures within the VPDMf Digital Library.
 * 
 * @author burns
 *
 */
public class JournalBuilder 
{	

	private static Logger logger = Logger.getLogger(JournalBuilder.class);
	
	private static String USAGE = "JournalBuilder <sheet-dir>";
	
	private static String FTP = "ftp://ftp.ncbi.nih.gov/pubmed/J_Entrez.txt";	
	private static String PUBMED = "pubmed";
	private static String JOURNAL = "Journal";
	
	private String baseQueryPrefix = "http://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pmc&id=";
	private String baseQuerySuffix = "&retmode=xml&rettype=MEDLINE";
	
	private String pubmedId;
	private int pageCapacity = 100;
	private int startOfPage = 0;
	private int counter;
	private Date lastQuery;
	
	public static void main(String[] args) throws Exception {
		
		if( args.length != 1 ) {
			System.err.println(USAGE);
			System.exit(-1);
		}

		File f = new File(args[0]);
		if( !f.exists() ) {
			System.err.println(f.getPath() + " does not exist.");
			System.err.println(USAGE);
			System.exit(-1);			
		}
			
		JournalBuilder jb = new JournalBuilder();
		jb.dumpJournalListToTxtFiles(f);
		
	}
	
	/**
	 * Dumps a ViewTable.txt and a Journal.txt file to designated directory
	 * @param dir
	 */
	public void dumpJournalListToTxtFiles(File dir) throws Exception {	

		URL archiveUrl = this.getClass().getClassLoader().getResource(
				"edu/isi/bmkeg/digitalLibrary/digitalLibrary_VPDMf.zip");
		File archiveFile = new File( archiveUrl.getPath() );
		
		VPDMfKnowledgeBaseBuilder builder = new VPDMfKnowledgeBaseBuilder(archiveFile, 
				null, null, null);
		VPDMf top = builder.readTop();
		ClassLoader cl = JournalBuilder.class.getClassLoader();
		
		List<Journal> jList = this.buildJournalListFromPubmed();
		
		File vtf = new File(dir.getPath() + "/ViewTable.txt");
		File jf = new File(dir.getPath() + "/Journal.txt");
		
		FileWriter vtfw = new FileWriter(vtf.getAbsoluteFile());
		BufferedWriter vtbw = new BufferedWriter(vtfw);
		vtbw.write("bmkegId	viewType	locked	vpdmfLabel	namespace	thumbnail\n");
		
		FileWriter jfw = new FileWriter(jf.getAbsoluteFile());
		BufferedWriter jbw = new BufferedWriter(jfw);
		jbw.write("bmkegId	journalTitle	nlmId	abbr	ISSN\n");
		
		Iterator<Journal> jIt = jList.iterator();
		int i = 0;
		while( jIt.hasNext() ) {
			Journal j = jIt.next();
			
			if( j.getAbbr() == null || j.getAbbr().length() == 0 )
				continue;
			
			j.setVpdmfLabel(j.getAbbr() + ".");
			j.setViewType(".Journal.%");
			i++;
			
			if( j.getISSN() == null || j.getISSN().length() == 0)
				j.setISSN("XXXX-XXXX");
						
			String s1 = i + "\t" + j.getViewType() + "\t\\N\t" + j.getAbbr() + ".\t\\N\t\\N";
			vtbw.write( s1.replaceAll("\n", "") + "\n" );
			String s2 = i + "\t" + j.getJournalTitle() + "\t" + j.getNlmId() + "\t" + j.getAbbr() + "\t" + j.getISSN();
			jbw.write( s2.replaceAll("\n", "") + "\n" );
			
		}
		
		vtbw.close();
		jbw.close();
		
	}	
	
	private List<Journal> buildJournalListFromPubmed() throws Exception {
				
		List<Journal> journals = new ArrayList<Journal>();	
		Journal j = null;	
		
		URL url = new URL(FTP);		
        BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()));
       
        Pattern p = Pattern.compile("(JournalTitle|IsoAbbr|ISSN \\(Print\\)|NlmId): (.*)$");
        
        String thisLine;
        while ((thisLine = in.readLine()) != null) { 
        	
        	if( thisLine.equals("--------------------------------------------------------") ) {
        		
        		if( j != null )
        			journals.add(j);
        		
        		j = new Journal();
        		continue;
        		
        	} 
        	        	
        	Matcher m = p.matcher(thisLine);
        	if( m.find() ) {
        		
        		String key = m.group(1);
        		String value = m.group(2);
        		
        		if( key.equals("JournalTitle") ) {
                	
            		j.setJournalTitle(value);
            		
            	} else if(  key.equals("IsoAbbr")) {

            		j.setAbbr(value);

            	} else if(  key.equals("ISSN (Print)")) {

            		j.setISSN(value);

            	} else if(  key.equals("NlmId")) {

            		j.setNlmId(value);
            	
            	}
        		
        	}
        	        	
        }
        
        return journals;
    
	}

}
