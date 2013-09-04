package edu.isi.bmkeg.digitalLibrary.dao.vpdmf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import edu.isi.bmkeg.digitalLibrary.dao.impl.ExtendedDigitalLibraryDaoImpl;
import edu.isi.bmkeg.digitalLibrary.model.citations.ArticleCitation;
import edu.isi.bmkeg.digitalLibrary.model.citations.Author;
import edu.isi.bmkeg.digitalLibrary.model.citations.Corpus;
import edu.isi.bmkeg.digitalLibrary.model.citations.ID;
import edu.isi.bmkeg.digitalLibrary.model.citations.Journal;
import edu.isi.bmkeg.digitalLibrary.model.citations.Keyword;
import edu.isi.bmkeg.digitalLibrary.model.citations.URL;
import edu.isi.bmkeg.vpdmf.model.instances.LightViewInstance;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"/Testcontext-bmkeg.xml","/applicationContext-daoVpdmf.xml"})
@DirtiesContext(classMode=ClassMode.AFTER_EACH_TEST_METHOD) // Forcing the initialization of the ApplicationContext after each test.
															// This is needed to provide a clean dao instance and a blank db which is
															// produced during the application context initialization.
public class VpdmfCitationsDaoTest {

	@Autowired
	private ExtendedDigitalLibraryDaoImpl cdao;

    public void setCitationsDao(ExtendedDigitalLibraryDaoImpl citationsDao) {
        this.cdao = citationsDao;
    }

	@Before
	public void setUp() throws Exception {

	}

	@After
	public void tearDown() throws Exception {

	}
	
	@Test
	public void testAddFindAndUpdateArticle() throws Exception {
	
		
	}
	
	// TODO UPDATE THESE TESTS TO THE ExtendedDigitalLibraryDao design.
	
	/*
		Journal j1 = createTestJournal();
		
		Assert.assertEquals("Journal id should be 0 before inserting to db",0, j1.getVpdmfId());
		
		// Inserts journal
		cdao.insertJournal(j1);
		
		long j1Id = j1.getVpdmfId();
		Assert.assertFalse("Id of inserted hjournal shouldn't be 0", j1Id == 0);

		ArticleCitation a1 = createTestArticle(j1);
	
		Assert.assertEquals("Article id should be 0 before inserting to db",0, a1.getVpdmfId());
		
		// Inserts article
		cdao.insertArticleCitation(a1);
		
		long a1Id = a1.getVpdmfId();
		Assert.assertFalse("Id of inserted article shouldn't be 0", a1Id == 0);
		
		// Finds article by Id	
		ArticleCitation a2 = cdao.findArticleByVpdmfId(a1Id);
		Assert.assertNotNull("Article 2 shouldnt be null", a2);
		assertArticlesDeepEquals(a1, a2);
		
		// Updates article
		a1.setTitle("Modified title");		
		cdao.updateArticleCitation(a1);

		Assert.assertEquals("Id of updated article shouldnt have changed", a1Id, a1.getVpdmfId());

		// Finds updated article	
		a2 = cdao.findArticleByVpdmfId(a1Id);
		Assert.assertNotNull("Article 2 shouldnt be null", a2);
		Assert.assertEquals("Modified title",a2.getTitle());
		assertArticlesDeepEquals(a1, a2);	
	}

	@Test
	public void testFindArticleByPmid() throws Exception {
	
		Journal j1 = createTestJournal();
		
		Assert.assertEquals("Journal id should be 0 before inserting to db",0, j1.getVpdmfId());
		
		// Inserts journal
		cdao.insertJournal(j1);
		
		long j1Id = j1.getVpdmfId();
		Assert.assertFalse("Id of inserted hjournal shouldn't be 0", j1Id == 0);

		ArticleCitation a1 = createTestArticle(j1);
	
		Assert.assertEquals("Article id should be 0 before inserting to db",0, a1.getVpdmfId());
		
		// Inserts article
		cdao.insertArticleCitation(a1);
		
		long a1Id = a1.getVpdmfId();
		Assert.assertFalse("Id of inserted article shouldn't be 0", a1Id == 0);
	
		// Finds article by Pmid	
		ArticleCitation a2 = cdao.findArticleByPmid(a1.getPmid());
		Assert.assertNotNull("Article 2 shouldnt be null", a2);
		assertArticlesDeepEquals(a1, a2);
	}

	@Test  @Ignore("vpdmf.deleteView is not working yet")
	public void testDeleteArticle() throws Exception {
	
		ArticleCitation a1 = createTestArticle();
	
		Assert.assertEquals("Article id should be 0 before inserting to db",0, a1.getVpdmfId());
		
		// Inserts article
		cdao.insertArticleCitation(a1);
		
		long a1Id = a1.getVpdmfId();
		Assert.assertFalse("Id of inserted article shouldn't be 0", a1Id == 0);
		
		// Finds article by Id	
		ArticleCitation a2 = cdao.findArticleByVpdmfId(a1Id);
		Assert.assertNotNull("Article 2 shouldnt be null",a2);

		assertArticlesDeepEquals(a1, a2);
		
		// Delete article
		cdao.deleteArticleCitation(a1Id);
		
		// Attempts to retrieve deleted article.
		try {
			a2 = cdao.findArticleByVpdmfId(a1Id);
			Assert.fail("Should have thrown an exception");			
		} catch (Exception e) {}
	
	}

	@Test
	public void testFindNonexistingArticleById() throws Exception {

		long aId = 999999;
				
		try {
			cdao.findArticleByVpdmfId(aId);
			Assert.fail("Should have thrown an exception");
		} catch (Exception e) {}
	}
	
	@Test
	public void testRetrieveArticlesPaged() throws Exception {
	
		int cnt = 10;
		
		Journal j = new Journal();
		j.setJournalTitle("Journal 1");
		j.setAbbr("j1");

		// Inserts journal
		cdao.insertJournal(j);
		
		long jId = j.getVpdmfId();
		Assert.assertFalse("Id of inserted hjournal shouldn't be 0", jId == 0);

		Author p = new Author();
		p.setFullName("Person1");

		
		// Inserts cnt articles
		for (int i = 0; i < cnt; i++) {
			ArticleCitation a = new ArticleCitation();

			a.setTitle(i + "Test article ");
			a.setAuthorList(new ArrayList<Author>());
			a.getAuthorList().add(p);
			a.setPubYear(2000);
			a.setVolume(String.valueOf(i));
			a.setPages("1");
			a.setJournal(j);
			
			a.setKeywordList(new ArrayList<Keyword>());
			Keyword k = new Keyword();
			k.setValue("k1");
			a.getKeywordList().add(k);
			
			a.setPmid(5678+i);
			
			ID id = new ID();
			id.setIdType("ISBN");
			id.setIdValue("isbn2");
			a.getIds().add(id);

			a.setFullTextUrl(new ArrayList<URL>());
			URL u = new URL();
			u.setUrl("http://article2URL");
			a.getFullTextUrl().add(u);

			cdao.insertArticleCitation(a);
			
		}
		
		// Retrieves cnt articles
		List<ArticleCitation> l = cdao.retrieveAllArticlesPaged(3,4);
		
		Assert.assertNotNull("List of articles shouldnt be null", l);
		Assert.assertEquals("There should be 4 articles", 4, l.size());
		
	}

	@Test
	public void testRetrieveAllArticles() throws Exception {
	
		Journal j1 = createTestJournal();
		
		Assert.assertEquals("Journal id should be 0 before inserting to db",0, j1.getVpdmfId());
		
		// Inserts journal
		cdao.insertJournal(j1);
		
		long j1Id = j1.getVpdmfId();
		Assert.assertFalse("Id of inserted hjournal shouldn't be 0", j1Id == 0);

		Journal j2 = createTestJournal2();
		
		Assert.assertEquals("Journal id should be 0 before inserting to db",0, j2.getVpdmfId());
		
		// Inserts journal
		cdao.insertJournal(j2);
		
		long j2Id = j2.getVpdmfId();
		Assert.assertFalse("Id of inserted hjournal shouldn't be 0", j2Id == 0);

		ArticleCitation a1 = createTestArticle(j1);
		ArticleCitation a2 = createTestArticle2(j2);
		
		// Inserts two articles
		cdao.insertArticleCitation(a1);
		long a1Id = a1.getVpdmfId();
		Assert.assertFalse("Id of inserted article shouldn't be 0", a1Id == 0);

		cdao.insertArticleCitation(a2);
		long a2Id = a2.getVpdmfId();
		Assert.assertFalse("Id of inserted article shouldn't be 0", a2Id == 0);
		
		// List Articles
		List<ArticleCitation> l = cdao.retrieveAllArticles();
		
		Assert.assertNotNull("List of articles shouldnt be null", l);
		Assert.assertEquals("There should be 2 articles", 2, l.size());
		
		for (ArticleCitation a : l) {
			if (a.getVpdmfId() == a1Id) 
				assertArticlesDeepEquals(a1, a);
			else if (a.getVpdmfId() == a2Id) 
				assertArticlesDeepEquals(a2, a);
			else
				Assert.fail("Unexpected article id");
		}
	}

	@Test  @Ignore("Exception thrown if there is no Articles in DB")
	public void testListAllArticlesInEmptyDB() throws Exception {
	
		List<ArticleCitation> l = cdao.retrieveAllArticles();
		
		Assert.assertNotNull("List of articles shouldnt be null", l);
		Assert.assertEquals("There should be 0 articles", 0, l.size());
		
	}

	@Test
	public void testAddFindAndUpdateJournal() throws Exception {
		
		Journal j1 = createTestJournal();
	
		Assert.assertEquals("Journal id should be 0 before inserting to db",0, j1.getVpdmfId());
		
		// Inserts journal
		cdao.insertJournal(j1);
		
		long j1Id = j1.getVpdmfId();
		Assert.assertFalse("Id of inserted hjournal shouldn't be 0", j1Id == 0);
		
		// Finds journal by Id	
		Journal j2 = cdao.findJournalById(j1Id);
		Assert.assertNotNull("Journal 2 shouldnt be null", j2);
		assertJournalsDeepEquals(j1, j2);
		
		// Updates article
		j1.setJournalTitle("Modified title");		
		cdao.updateJournal(j1);

		Assert.assertEquals("Id of updated journal shouldnt have changed", j1Id, j1.getVpdmfId());

		// Finds updated article	
		j2 = cdao.findJournalById(j1Id);
		Assert.assertNotNull("Jornal 2 shouldnt be null", j2);
		Assert.assertEquals("Modified title",j2.getJournalTitle());
		assertJournalsDeepEquals(j1, j2);	
	}

	@Test
	public void testRetrieveAllJournals() throws Exception {
	
		// List Journals
		List<Journal> l = cdao.retrieveAllJournalsPaged(0, 100);
		
		Assert.assertNotNull("List of journal shouldnt be null", l);
		Assert.assertEquals("There should be 100 journals, not just: " + l.size(),100,l.size() );
		
	}

	@Test
	public void testListAllJournals() throws Exception {
	
		// List Journals
		List<LightViewInstance> l = cdao.listAllJournalsPaged(0, 100);
		
		Assert.assertNotNull("List of journal shouldnt be null", l);
		Assert.assertEquals("There should be 100 journals, not just: " + l.size(),100,l.size() );
		
	}

	@Test
	public void testListMatchingJournals() throws Exception {
	
		// List Journals
		List<LightViewInstance> l = cdao.listMatchingJournalsAbbrPaged("%Acta%", 0, 5);
		
		Assert.assertNotNull("List of journal shouldnt be null", l);

		Assert.assertEquals("There should be 5 journals, not just: " + l.size(),5,l.size() );
		
		for (LightViewInstance li : l) {
			Assert.assertTrue("List Item label should contain 'Acta': " + li.getVpdmfLabel(),li.getVpdmfLabel().contains("Acta"));
		}
		
	}

	@Test
	public void testFindJournalByAbbr() throws Exception {
	
		String jabbr = "J Comput Neurosci";

		Journal j = cdao.findJournalByAbbr(jabbr);
		
		Assert.assertNotNull("journal shouldnt be null", j);

		Assert.assertEquals(jabbr,j.getAbbr());		
	}

	@Test
	public void testFindJournalByAbbrFail() throws Exception {
	
		String jabbr = "XXX";

		Journal j = cdao.findJournalByAbbr(jabbr);
		
		Assert.assertNull("journal should be null", j);
	}

	@Test
	public void testListMatchingJournalsEmpty() throws Exception {
	
		// List Journals
		List<LightViewInstance> l = cdao.listMatchingJournalsAbbrPaged("%xyz%", 0, 5);
		
		Assert.assertNotNull("List of journal shouldnt be null", l);

		Assert.assertEquals("There should be 0 journals, not just: " + l.size(),0,l.size() );
				
	}

	@Test
	public void testAddFindAndUpdateCorpus() throws Exception {
	

		Corpus c1 = createTestCorpus1();
	
		Assert.assertEquals("Corpus id should be 0 before inserting to db",0, c1.getVpdmfId());
		
		// Inserts corpus
		cdao.insertCorpus(c1);
		
		long c1Id = c1.getVpdmfId();
		Assert.assertFalse("Id of inserted article shouldn't be 0", c1Id == 0);
		
		// Finds corpus by Id	
		Corpus c2 = cdao.findCorpusById(c1Id);
		Assert.assertNotNull("Corpus 2 shouldnt be null", c2);
		assertCorpusDeepEquals(c1, c2);
		
		// Updates corpus
		c1.setDescription("Modified title");		
		cdao.updateCorpus(c1);

		Assert.assertEquals("Id of updated corpus shouldnt have changed", c1Id, c1.getVpdmfId());

		// Finds updated article	
		c2 = cdao.findCorpusById(c1Id);
		Assert.assertNotNull("Corpus 2 shouldnt be null", c2);
		Assert.assertEquals("Modified title",c2.getDescription());
		assertCorpusDeepEquals(c1, c2);	
	}
	
	@Test
	public void testAddCorpusToArticle() throws Exception {
	

		// Create Journal
		Journal j = new Journal();
		j.setJournalTitle("Journal 1");
		j.setAbbr("j1");

		// Inserts journal
		cdao.insertJournal(j);
		
		long jId = j.getVpdmfId();
		Assert.assertFalse("Id of inserted hjournal shouldn't be 0", jId == 0);

		// Creates an Article
		ArticleCitation a1 = createTestArticle(j);
		
		// Inserts article
		cdao.insertArticleCitation(a1);
		
		long a1Id = a1.getVpdmfId();
		Assert.assertFalse("Id of inserted article shouldn't be 0", a1Id == 0);
		
		// Retrieve article
		ArticleCitation a2 = cdao.findArticleByVpdmfId(a1Id);
		Assert.assertNotNull("Article 2 shouldnt be null", a2);
		
		// Check that article has no corpus
		Assert.assertTrue("Article has no corpora", a2.getCorpora() == null || a2.getCorpora().size() == 0);
		
		// Creates a corpus
		Corpus c1 = createTestCorpus1();
	
		// Inserts corpus
		cdao.insertCorpus(c1);
		
		long c1Id = c1.getVpdmfId();
		Assert.assertFalse("Id of inserted article shouldn't be 0", c1Id == 0);
		
		// Add corpus to article
		cdao.addCorpusToArticle(a1Id, c1Id);
		
		// Retrieve article
		a2 = cdao.findArticleByVpdmfId(a1Id);
		Assert.assertNotNull("Article 2 shouldnt be null", a2);
		
		// Check that article has only 1 corpus
		Assert.assertEquals(1, a2.getCorpora().size());
		Assert.assertEquals(c1Id,a2.getCorpora().get(0).getVpdmfId());
		
		// Creates another corpus
		Corpus c2 = createTestCorpus2();
	
		// Inserts corpus
		cdao.insertCorpus(c2);
		
		long c2Id = c2.getVpdmfId();
		Assert.assertFalse("Id of inserted article shouldn't be 0", c2Id == 0);
		
		// Add another corpus to article
		cdao.addCorpusToArticle(a1Id, c2Id);
		
		// Retrieve article
		a2 = cdao.findArticleByVpdmfId(a1Id);
		Assert.assertNotNull("Article 2 shouldnt be null", a2);
		
		// Check that article has only 2 corpus
		Assert.assertEquals(2, a2.getCorpora().size());
		
		Assert.assertEquals(c1Id,a2.getCorpora().get(0).getVpdmfId());
		Assert.assertEquals(c2Id,a2.getCorpora().get(1).getVpdmfId());
		
	}

	@Test
	public void testAddCorpusToArticles() throws Exception {
	

		// Create Journal
		Journal j = new Journal();
		j.setJournalTitle("Journal 1");
		j.setAbbr("j1");

		// Inserts journal
		cdao.insertJournal(j);
		
		long jId = j.getVpdmfId();
		Assert.assertFalse("Id of inserted hjournal shouldn't be 0", jId == 0);

		// Creates an Article
		ArticleCitation a1 = createTestArticle(j);
		
		// Inserts article
		cdao.insertArticleCitation(a1);
		
		long a1Id = a1.getVpdmfId();
		Assert.assertFalse("Id of inserted article shouldn't be 0", a1Id == 0);
		
		// Creates another Article
		ArticleCitation a2 = createTestArticle2(j);
		
		// Inserts article
		cdao.insertArticleCitation(a2);
		
		long a2Id = a2.getVpdmfId();
		Assert.assertFalse("Id of inserted article shouldn't be 0", a2Id == 0);

		// Creates a corpus
		Corpus c1 = createTestCorpus1();
	
		// Inserts corpus
		cdao.insertCorpus(c1);
		
		long c1Id = c1.getVpdmfId();
		Assert.assertFalse("Id of inserted article shouldn't be 0", c1Id == 0);
		
		// Add corpus to articles
		long[] as = new long[2];
		as[0] = a1Id;
		as[1] = a2Id;
		cdao.addCorpusToArticles(c1Id, as);
		
		// Retrieve article
		ArticleCitation a = cdao.findArticleByVpdmfId(a1Id);
		Assert.assertNotNull("Article shouldnt be null", a);
		
		// Check that article has only 1 corpus
		Assert.assertEquals(1, a.getCorpora().size());
		Assert.assertEquals(c1Id,a.getCorpora().get(0).getVpdmfId());

		// Retrieve second article
		a = cdao.findArticleByVpdmfId(a2Id);
		Assert.assertNotNull("Article shouldnt be null", a);
		
		// Check that the other article has only 1 corpus
		Assert.assertEquals(1, a.getCorpora().size());
		Assert.assertEquals(c1Id,a.getCorpora().get(0).getVpdmfId());
	}

	@Test
	public void testInsertArticleWithCorpus() throws Exception {
	

		// Creates a corpus
		Corpus c1 = createTestCorpus1();
	
		// Inserts corpus
		cdao.insertCorpus(c1);
		
		long c1Id = c1.getVpdmfId();
		Assert.assertFalse("Id of inserted article shouldn't be 0", c1Id == 0);
		
		// Create Journal
		Journal j = new Journal();
		j.setJournalTitle("Journal 1");
		j.setAbbr("j1");

		// Inserts journal
		cdao.insertJournal(j);
		
		long jId = j.getVpdmfId();
		Assert.assertFalse("Id of inserted hjournal shouldn't be 0", jId == 0);

		// Creates an Article
		ArticleCitation a1 = createTestArticle(j);
		a1.setCorpora(new ArrayList<Corpus>());
		a1.getCorpora().add(c1);
		
		// Inserts article
		cdao.insertArticleCitation(a1);
		
		long a1Id = a1.getVpdmfId();
		Assert.assertFalse("Id of inserted article shouldn't be 0", a1Id == 0);
		
		// Retrieve article
		ArticleCitation a2 = cdao.findArticleByVpdmfId(a1Id);
		Assert.assertNotNull("Article 2 shouldnt be null", a2);
				
		// Check that article has only 1 corpus
		Assert.assertEquals(1, a2.getCorpora().size());
		Assert.assertEquals(c1Id,a2.getCorpora().get(0).getVpdmfId());
				
	}
	
	@Test @Ignore("Fails because DAO should not relay in executeFullQuery")
	public void testRetrieveCorpusArticlesPaged() throws Exception {
	
		int c0cnt = 2;	// number of articles with no corpus
		int c1cnt = 3;	// number of articles in corpus 1 exclusively
		int c2cnt = 4;	// number of articles in corpus 2 exclusively
		int c12cnt = 5; // number of articles in both, corpus 1 and 2
		
		// Creates a corpus
		Corpus c1 = createTestCorpus1();
	
		// Inserts corpus
		cdao.insertCorpus(c1);
		
		long c1Id = c1.getVpdmfId();
		String c1Name = c1.getName();
		Assert.assertFalse("Id of inserted article shouldn't be 0", c1Id == 0);

		// Creates another corpus
		Corpus c2 = createTestCorpus2();
	
		// Inserts corpus
		cdao.insertCorpus(c2);
		
		long c2Id = c2.getVpdmfId();
		String c2Name = c2.getName();
		Assert.assertFalse("Id of inserted article shouldn't be 0", c2Id == 0);

		Journal j = new Journal();
		j.setJournalTitle("Journal 1");
		j.setAbbr("j1");

		// Inserts journal
		cdao.insertJournal(j);
		
		long jId = j.getVpdmfId();
		Assert.assertFalse("Id of inserted hjournal shouldn't be 0", jId == 0);

		Author p = new Author();
		p.setFullName("Person1");

		
		// Inserts articles with no corpus
		int i0 = 0;
		int i1 = i0 + c0cnt;
		
		for (int i = i0; i < i1; i++) {
			ArticleCitation a = new ArticleCitation();

			a.setTitle(i + "Test article ");
			a.setAuthorList(new ArrayList<Author>());
			a.getAuthorList().add(p);
			a.setPubYear(2000);
			a.setVolume(String.valueOf(i));
			a.setPages("1");
			a.setJournal(j);
			
			a.setKeywordList(new ArrayList<Keyword>());
			Keyword k = new Keyword();
			k.setValue("k1");
			a.getKeywordList().add(k);
			
			a.setPmid(5678+i);
			
			ID id = new ID();
			id.setIdType("ISBN");
			id.setIdValue("isbn2");
			a.getIds().add(id);

			a.setFullTextUrl(new ArrayList<URL>());
			URL u = new URL();
			u.setUrl("http://article2URL");
			a.getFullTextUrl().add(u);

			cdao.insertArticleCitation(a);
			
		}
		
		// Inserts articles with corpus c1
		
		List<Corpus> cs = new ArrayList<Corpus>();
		cs.add(c1);

		i0 = i1;
		i1 = i0 + c1cnt;
		
		for (int i = i0; i < i1; i++) {
			ArticleCitation a = new ArticleCitation();

			a.setTitle(i + "Test article ");
			a.setAuthorList(new ArrayList<Author>());
			a.getAuthorList().add(p);
			a.setPubYear(2000);
			a.setVolume(String.valueOf(i));
			a.setPages("1");
			a.setJournal(j);
			
			a.setKeywordList(new ArrayList<Keyword>());
			Keyword k = new Keyword();
			k.setValue("k1");
			a.getKeywordList().add(k);
			
			a.setPmid(5678+i);
			
			ID id = new ID();
			id.setIdType("ISBN");
			id.setIdValue("isbn2");
			a.getIds().add(id);

			a.setFullTextUrl(new ArrayList<URL>());
			URL u = new URL();
			u.setUrl("http://article2URL");
			a.getFullTextUrl().add(u);
			
			a.setCorpora(cs);

			cdao.insertArticleCitation(a);
			
		}

		// Inserts articles with corpus c2
		
		cs = new ArrayList<Corpus>();
		cs.add(c2);

		i0 = i1;
		i1 = i0 + c2cnt;
		
		for (int i = i0; i < i1; i++) {
			ArticleCitation a = new ArticleCitation();

			a.setTitle(i + "Test article ");
			a.setAuthorList(new ArrayList<Author>());
			a.getAuthorList().add(p);
			a.setPubYear(2000);
			a.setVolume(String.valueOf(i));
			a.setPages("1");
			a.setJournal(j);
			
			a.setKeywordList(new ArrayList<Keyword>());
			Keyword k = new Keyword();
			k.setValue("k1");
			a.getKeywordList().add(k);
			
			a.setPmid(5678+1000+i);
			
			ID id = new ID();
			id.setIdType("ISBN");
			id.setIdValue("isbn2");
			a.getIds().add(id);

			a.setFullTextUrl(new ArrayList<URL>());
			URL u = new URL();
			u.setUrl("http://article2URL");
			a.getFullTextUrl().add(u);
			
			a.setCorpora(cs);

			cdao.insertArticleCitation(a);
			
		}

		// Inserts articles with corpus c1 and c2
		
		cs = new ArrayList<Corpus>();
		cs.add(c1);
		cs.add(c2);

		i0 = i1;
		i1 = i0 + c12cnt;
		
		for (int i = i0; i < i1; i++) {
			ArticleCitation a = new ArticleCitation();

			a.setTitle(i + "Test article ");
			a.setAuthorList(new ArrayList<Author>());
			a.getAuthorList().add(p);
			a.setPubYear(2000);
			a.setVolume(String.valueOf(i));
			a.setPages("1");
			a.setJournal(j);
			
			a.setKeywordList(new ArrayList<Keyword>());
			Keyword k = new Keyword();
			k.setValue("k1");
			a.getKeywordList().add(k);
			
			a.setPmid(5678+2000+i);
			
			ID id = new ID();
			id.setIdType("ISBN");
			id.setIdValue("isbn2");
			a.getIds().add(id);

			a.setFullTextUrl(new ArrayList<URL>());
			URL u = new URL();
			u.setUrl("http://article2URL");
			a.getFullTextUrl().add(u);
			
			a.setCorpora(cs);

			cdao.insertArticleCitation(a);
			
		}

		// Retrieves articles in c1
		List<ArticleCitation> l = cdao.retrieveCorpusArticlesPaged(c1Name,0,100);
		Assert.assertNotNull("List of articles shouldnt be null", l);
		Assert.assertEquals("There should be c1cnt + c12cnt articles", c1cnt + c12cnt, l.size());

		// Retrieves articles in c2
		l = cdao.retrieveCorpusArticlesPaged(c2Name,0,100);
		Assert.assertNotNull("List of articles shouldnt be null", l);
		Assert.assertEquals("There should be c2cnt + c12cnt articles", c2cnt + c12cnt, l.size());

	}

	@Test
	public void testCountCorpusArticles() throws Exception {
	
		int c0cnt = 2;	// number of articles with no corpus
		int c1cnt = 3;	// number of articles in corpus 1 exclusively
		int c2cnt = 4;	// number of articles in corpus 2 exclusively
		int c12cnt = 5; // number of articles in both, corpus 1 and 2
		
		// Creates a corpus
		Corpus c1 = createTestCorpus1();
	
		// Inserts corpus
		cdao.insertCorpus(c1);
		
		long c1Id = c1.getVpdmfId();
		String c1Name = c1.getName();
		Assert.assertFalse("Id of inserted article shouldn't be 0", c1Id == 0);

		// Creates another corpus
		Corpus c2 = createTestCorpus2();
	
		// Inserts corpus
		cdao.insertCorpus(c2);
		
		long c2Id = c2.getVpdmfId();
		String c2Name = c2.getName();
		Assert.assertFalse("Id of inserted article shouldn't be 0", c2Id == 0);

		Journal j = new Journal();
		j.setJournalTitle("Journal 1");
		j.setAbbr("j1");

		// Inserts journal
		cdao.insertJournal(j);
		
		long jId = j.getVpdmfId();
		Assert.assertFalse("Id of inserted hjournal shouldn't be 0", jId == 0);

		Author p = new Author();
		p.setFullName("Person1");

		
		// Inserts articles with no corpus
		int i0 = 0;
		int i1 = i0 + c0cnt;
		
		for (int i = i0; i < i1; i++) {
			ArticleCitation a = new ArticleCitation();

			a.setTitle(i + "Test article ");
			a.setAuthorList(new ArrayList<Author>());
			a.getAuthorList().add(p);
			a.setPubYear(2000);
			a.setVolume(String.valueOf(i));
			a.setPages("1");
			a.setJournal(j);
			
			a.setKeywordList(new ArrayList<Keyword>());
			Keyword k = new Keyword();
			k.setValue("k1");
			a.getKeywordList().add(k);
			
			a.setPmid(5678+i);
			
			ID id = new ID();
			id.setIdType("ISBN");
			id.setIdValue("isbn2");
			a.getIds().add(id);

			a.setFullTextUrl(new ArrayList<URL>());
			URL u = new URL();
			u.setUrl("http://article2URL");
			a.getFullTextUrl().add(u);

			cdao.insertArticleCitation(a);
			
		}
		
		// Inserts articles with corpus c1
		
		List<Corpus> cs = new ArrayList<Corpus>();
		cs.add(c1);

		i0 = i1;
		i1 = i0 + c1cnt;
		
		for (int i = i0; i < i1; i++) {
			ArticleCitation a = new ArticleCitation();

			a.setTitle(i + "Test article ");
			a.setAuthorList(new ArrayList<Author>());
			a.getAuthorList().add(p);
			a.setPubYear(2000);
			a.setVolume(String.valueOf(i));
			a.setPages("1");
			a.setJournal(j);
			
			a.setKeywordList(new ArrayList<Keyword>());
			Keyword k = new Keyword();
			k.setValue("k1");
			a.getKeywordList().add(k);
			
			a.setPmid(5678+i);
			
			ID id = new ID();
			id.setIdType("ISBN");
			id.setIdValue("isbn2");
			a.getIds().add(id);

			a.setFullTextUrl(new ArrayList<URL>());
			URL u = new URL();
			u.setUrl("http://article2URL");
			a.getFullTextUrl().add(u);
			
			a.setCorpora(cs);

			cdao.insertArticleCitation(a);
			
		}

		// Inserts articles with corpus c2
		
		cs = new ArrayList<Corpus>();
		cs.add(c2);

		i0 = i1;
		i1 = i0 + c2cnt;
		
		for (int i = i0; i < i1; i++) {
			ArticleCitation a = new ArticleCitation();

			a.setTitle(i + "Test article ");
			a.setAuthorList(new ArrayList<Author>());
			a.getAuthorList().add(p);
			a.setPubYear(2000);
			a.setVolume(String.valueOf(i));
			a.setPages("1");
			a.setJournal(j);
			
			a.setKeywordList(new ArrayList<Keyword>());
			Keyword k = new Keyword();
			k.setValue("k1");
			a.getKeywordList().add(k);
			
			a.setPmid(5678+1000+i);
			
			ID id = new ID();
			id.setIdType("ISBN");
			id.setIdValue("isbn2");
			a.getIds().add(id);

			a.setFullTextUrl(new ArrayList<URL>());
			URL u = new URL();
			u.setUrl("http://article2URL");
			a.getFullTextUrl().add(u);
			
			a.setCorpora(cs);

			cdao.insertArticleCitation(a);
			
		}

		// Inserts articles with corpus c1 and c2
		
		cs = new ArrayList<Corpus>();
		cs.add(c1);
		cs.add(c2);

		i0 = i1;
		i1 = i0 + c12cnt;
		
		for (int i = i0; i < i1; i++) {
			ArticleCitation a = new ArticleCitation();

			a.setTitle(i + "Test article ");
			a.setAuthorList(new ArrayList<Author>());
			a.getAuthorList().add(p);
			a.setPubYear(2000);
			a.setVolume(String.valueOf(i));
			a.setPages("1");
			a.setJournal(j);
			
			a.setKeywordList(new ArrayList<Keyword>());
			Keyword k = new Keyword();
			k.setValue("k1");
			a.getKeywordList().add(k);
			
			a.setPmid(5678+2000+i);
			
			ID id = new ID();
			id.setIdType("ISBN");
			id.setIdValue("isbn2");
			a.getIds().add(id);

			a.setFullTextUrl(new ArrayList<URL>());
			URL u = new URL();
			u.setUrl("http://article2URL");
			a.getFullTextUrl().add(u);
			
			a.setCorpora(cs);

			cdao.insertArticleCitation(a);
			
		}
		
		// Retrieves articles in c1
		int cnt = cdao.countCorpusArticles(c1Name);
		Assert.assertEquals("There should be c1cnt + c12cnt articles", c1cnt + c12cnt, cnt);

		// Retrieves articles in c2
		cnt = cdao.countCorpusArticles(c2Name);
		Assert.assertEquals("There should be c2cnt + c12cnt articles", c2cnt + c12cnt, cnt);
	}

	@Test
	public void testFindCorpusByName() throws Exception {
	
		// Creates a corpus
		Corpus c1 = createTestCorpus1();
	
		// Inserts corpus
		cdao.insertCorpus(c1);
		
		long c1Id = c1.getVpdmfId();
		String c1Name = c1.getName();
		Assert.assertFalse("Id of inserted article shouldn't be 0", c1Id == 0);

		// Creates another corpus
		Corpus c2 = createTestCorpus2();
	
		// Inserts corpus
		cdao.insertCorpus(c2);
		
		long c2Id = c2.getVpdmfId();
		Assert.assertFalse("Id of inserted article shouldn't be 0", c2Id == 0);

		// Finds corpus 1
		Corpus c1p = cdao.findCorpusByName(c1Name);
		
		Assert.assertNotNull("Corpus 1p shouldnt be null", c1p);
		assertCorpusDeepEquals(c1, c1p);
	}

	@Test
	public void testFindCorpusByNameFail() throws Exception {
	
		// Creates a corpus
		Corpus c1 = createTestCorpus1();
	
		// Inserts corpus
		cdao.insertCorpus(c1);
		
		long c1Id = c1.getVpdmfId();
		Assert.assertFalse("Id of inserted article shouldn't be 0", c1Id == 0);

		String name = "XXX";

		Corpus c = cdao.findCorpusByName(name);
		
		Assert.assertNull("corpus should be null", c);
	}



	@Test
	public void testListAllCorpus() throws Exception {
	
		int cnt = 10;
				
		// Inserts cnt corpus
		for (int i = 0; i < cnt; i++) {
			Corpus c = new Corpus();

			c.setDescription(i + "Corpus description ");
			c.setName(i + "Corpus Name");

			cdao.insertCorpus(c);
		}
		
		// Retrieves some corpus
		List<LightViewInstance> l = cdao.listAllCorporaPaged(3,4);
		
		Assert.assertNotNull("List of corpora shouldnt be null", l);
		Assert.assertEquals("There should be 4 corpora", 4, l.size());		
	}

	private Corpus createTestCorpus1() {
		Corpus c = new Corpus();
		
		c.setDescription("Descritpion for Corpus 1");
		c.setName("corpus1");
		
		return c;
	}
	
	private Corpus createTestCorpus2() {
		Corpus c = new Corpus();
		
		c.setDescription("Descritpion for Corpus 2");
		c.setName("corpus2");
		
		return c;
	}
	
	private Journal createTestJournal() {
		Journal j = new Journal();
		
		j.setJournalTitle("Journal 1");
		j.setAbbr("j1");
		j.setISSN("ISSN 1");

		return j;
	}

	private Journal createTestJournal2() {
		Journal j = new Journal();
		
		j.setJournalTitle("Journal 2");
		j.setAbbr("j2");
		j.setISSN("ISSN 2");

		return j;
	}

	public static ArticleCitation createTestArticle2() {
		Journal j = new Journal();
		j.setJournalTitle("Journal 2");
		j.setAbbr("j2");

		return createTestArticle2(j);
	}

	public static ArticleCitation createTestArticle2(Journal j) {
		ArticleCitation a = new ArticleCitation();
		
		// LiteratureCitation properties
		a.setAbstractText("Abstract Article 2");
		a.setTitle("2 Test article");
		a.setPubYear(2002);
		a.setPages("2");

		a.setKeywordList(new ArrayList<Keyword>());
		
		Keyword k = new Keyword();
		k.setValue("k1");
		a.getKeywordList().add(k);

		k = new Keyword();
		k.setValue("k2");
		a.getKeywordList().add(k);
		
		a.setPmid(5678);

		a.setIds(new ArrayList<ID>());

		ID id = new ID();
		id.setIdType("ISBN");
		id.setIdValue("isbn2");
		a.getIds().add(id);
		
		id = new ID();
		id.setIdType("DOI");
		id.setIdValue("doi2");
		a.getIds().add(id);
			
		a.setAuthorList(new ArrayList<Author>());
		Author p = new Author();
		p.setFullName("Person 2 Full Name");
		a.getAuthorList().add(p);

		a.setFullTextUrl(new ArrayList<URL>());
		URL u = new URL();
		u.setUrl("http://article2URL");
		a.getFullTextUrl().add(u);
		
		// ArticleCitation properties
		a.setVolume("v2");
		a.setIssue("i2");
		a.setJournal(j);
		
		return a;
	}

	public static ArticleCitation createTestArticle() {

		Journal j = new Journal();
		j.setJournalTitle("Journal 1");
		j.setAbbr("j1");

		return createTestArticle(j);
	}

	public static ArticleCitation createTestArticle(Journal j) {
		ArticleCitation a = new ArticleCitation();
		
		// LiteratureCitation properties
		a.setAbstractText("Abstract Article 1");
		a.setTitle("1 Test article");
		a.setPubYear(2001);
		a.setPages("1");

		a.setKeywordList(new ArrayList<Keyword>());
		
		Keyword k = new Keyword();
		k.setValue("k1");
		a.getKeywordList().add(k);

		k = new Keyword();
		k.setValue("k2");
		a.getKeywordList().add(k);

		a.setIds(new ArrayList<ID>());

		ID id = new ID();
		id.setIdType("ISBN");
		id.setIdValue("isbn1");
		a.getIds().add(id);
		
		id = new ID();
		id.setIdType("DOI");
		id.setIdValue("doi1");
		a.getIds().add(id);
			
		a.setAuthorList(new ArrayList<Author>());
		Author p = new Author();
		p.setFullName("Person 1 Full Name");
		a.getAuthorList().add(p);

		a.setFullTextUrl(new ArrayList<URL>());
		URL u = new URL();
		u.setUrl("http://article1URL");
		a.getFullTextUrl().add(u);
		
		// ArticleCitation properties
		a.setVolume("v1");
		a.setIssue("i1");
		a.setJournal(j);
		a.setPmid(1234);
		
		return a;
	}
	
	static public void assertArticlesDeepEquals(ArticleCitation a1, ArticleCitation a2) {

		Assert.assertEquals(a1.getVpdmfId(), a2.getVpdmfId());

		// LiteratureCitation properties

		Assert.assertEquals(a1.getAbstractText(), a2.getAbstractText());
		Assert.assertEquals(a1.getTitle(), a2.getTitle());
		Assert.assertEquals(a1.getPubYear(), a2.getPubYear());
		Assert.assertEquals(a1.getPages(), a2.getPages());

		List<Keyword> k1s = a1.getKeywordList();
		List<Keyword> k2s = a2.getKeywordList();
		if (k1s == null) Assert.assertNull(k2s == null);
		else {
			Assert.assertEquals(k1s.size(), k2s.size());
			HashSet<String> k1smap = new HashSet<String>();
			for (int i = 0; i < k1s.size(); i++) {
				k1smap.add(k1s.get(i).getValue());
			}
			for (int i = 0; i < k2s.size(); i++) {
				Assert.assertTrue(k1smap.contains(k2s.get(i).getValue()));
			}
		}

		List<ID> id1s = a1.getIds();
		List<ID> id2s = a2.getIds();
		if (id1s == null) Assert.assertNull(id2s == null);
		else {
			Assert.assertEquals(id1s.size(), id2s.size());
			HashMap<String,String> id1smap = new HashMap<String,String>();
			for (int i = 0; i < id1s.size(); i++) {
				id1smap.put(id1s.get(i).getIdType(),id1s.get(i).getIdValue());
			}
			for (int i = 0; i < id2s.size(); i++) {
				Assert.assertEquals(id1smap.get(id2s.get(i).getIdType()),id2s.get(i).getIdValue());
			}
		}

		List<Author> p1s = a1.getAuthorList();
		List<Author> p2s = a2.getAuthorList();
		if (p1s == null) Assert.assertNull(p2s == null);
		else {
			Assert.assertEquals(p1s.size(), p2s.size());
			for (int i = 0; i < p1s.size(); i++) {
				Author p1 = p1s.get(i);
				Author p2 = p2s.get(i);
				assertPersonsDeepEquals(p1, p2);
			}
		}
		
		List<URL> url1s = a1.getFullTextUrl();
		List<URL> url2s = a2.getFullTextUrl();
		if (url1s == null) Assert.assertNull(url2s == null);
		else {
			Assert.assertEquals(url1s.size(), url2s.size());
			HashSet<String> url1smap = new HashSet<String>();
			for (int i = 0; i < url1s.size(); i++) {
				url1smap.add(url1s.get(i).getUrl());
			}
			for (int i = 0; i < url2s.size(); i++) {
				Assert.assertTrue(url1smap.contains(url2s.get(i).getUrl()));
			}
		}
		
		// ArticleCitation properties
		
		Assert.assertEquals(a1.getVolume(), a2.getVolume());
		Assert.assertEquals(a1.getIssue(), a2.getIssue());

		if (a1.getJournal() == null) Assert.assertNull(a2.getJournal());
		else assertJournalsDeepEquals(a1.getJournal(), a2.getJournal());
		Assert.assertEquals(a1.getPmid(), a2.getPmid()) ;

	}
	
	static public void assertJournalsDeepEquals(Journal j1, Journal j2) {
		
		Assert.assertEquals(j1.getVpdmfId(), j2.getVpdmfId());
		Assert.assertEquals(j1.getJournalTitle(), j2.getJournalTitle());
		Assert.assertEquals(j1.getAbbr(), j2.getAbbr());
		Assert.assertEquals(j1.getISSN(), j2.getISSN());
	}

	static public void assertPersonsDeepEquals(Author p1, Author p2) {
		
		Assert.assertEquals(p1.getVpdmfId(), p2.getVpdmfId());
		Assert.assertEquals(p1.getFullName(), p2.getFullName());
	}

	static public void assertCorpusDeepEquals(Corpus c1, Corpus c2) {
		
		Assert.assertEquals(c1.getVpdmfId(), c2.getVpdmfId());
		Assert.assertEquals(c1.getDescription(), c2.getDescription());
		Assert.assertEquals(c1.getName(), c2.getName());
	}*/

}
