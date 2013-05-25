package edu.isi.bmkeg.digitalLibrary.utils.pubmed;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={ "/edu/isi/bmkeg/digitalLibrary/appCtx-VPDMfTest.xml"})
public class JournalBuilderTest {
	
	File outputDir;
	JournalBuilder jb;
	
	@Before
	public void setUp() throws Exception {
		
		outputDir = new File("target");
				
	}

	@After
	public void tearDown() throws Exception {
	}
	
	@Test
	public final void testRunExecWithFullPaths() throws Exception {
				
		jb = new JournalBuilder();
		
		jb.dumpJournalListToTxtFiles(outputDir);
				
	}
		
}

