package edu.isi.bmkeg.digitalLibrary.bin;

import java.io.File;
import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import edu.isi.bmkeg.digitalLibrary.dao.vpdmf.VpdmfCitationsDao;
import edu.isi.bmkeg.utils.springContext.AppContext;
import edu.isi.bmkeg.utils.springContext.BmkegProperties;
import edu.isi.bmkeg.vpdmf.controller.VPDMfKnowledgeBaseBuilder;
import edu.isi.bmkeg.vpdmf.test.VPDMfTestCase;

public class BuildNeuroscienceCorporaTest extends VPDMfTestCase {
	
	ApplicationContext ctx;
	
	File archiveFile, pmidFile_allChecked, triageCodes, pdfDir;
	VPDMfKnowledgeBaseBuilder builder;
	
	VpdmfCitationsDao dao;
	
	String queryString1, queryString2, queryString3;
	
	@Before
	public void setUp() throws Exception {
		
//		super.setUp("edu/isi/bmkeg/digitalLibrary/jcn2012_test_VPDMf.zip", false); 
		super.setUp("edu/isi/bmkeg/digitalLibrary/digitalLibrary-mysql.zip", true); 
			
		queryString1 = "(\"J Comp Neurol\"[TA])+AND+(2012[DP]+AND+(Smith[AU])";
		queryString2 = "(\"Brain Res\"[TA])+AND+(2012[DP])+AND+(Smith[AU])";

		queryString3 = "\"alzheimer's disease\" and (2012[dp] or 2011[dp] or 2010[dp] or 2009[dp] " +
				"or 2008[dp] or 2007[dp] or 2006[dp] or 2005[dp] or 2004[dp] or 2004[dp] or 2003[dp] " + 
				"or 2002[dp] or 2001[dp] or 2000[dp] or 1999[dp] or 1998[dp])";
		
//		queryString = "(\"Am J Hum Genet\"[TA] OR \"Arch Biochem Biophys\"[TA] " + 
//				"OR \"Cell Death Differ\"[TA] OR \"Diabetes\"[TA] OR \"Exp Eye Res\"[TA] " + 
//				"OR \"Exp Gerontol\"[TA] OR \"Exp Hematol\"[TA] OR \"Genes Cells\"[TA]" +
//				"OR \"Invest Ophthalmol Vis Sci\"[TA] OR \"J Histochem Cytochem\"[TA]" +
//				"OR \"Mol Cell Neurosci\"[TA])+AND+(2000[DP]+OR+2001[DP]+OR+2002[DP]+" + 
//				"OR+2003[DP]+OR+2004[DP]+OR+2005[DP]+OR+2006[DP]+OR+2007[DP]+OR+2008[DP]" + 
//				"+OR+2009[DP]+OR+2010[DP]+OR+2011[DP]+OR+2012[DP])";
				
	}

	@After
	public void tearDown() throws Exception {
		
		super.tearDown();
		
	}
	
	@Test
	public final void testBuildTriageCorpusFromScratch() throws Exception {

		String[] args = new String[] { 
				"Brain Res.", queryString2, 
				this.getDbUrl(), this.getLogin(), this.getPassword()
				};

		BuildCorpusFromMedlineQuery.main(args);
						
	}
		
}

