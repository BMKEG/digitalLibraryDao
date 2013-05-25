package edu.isi.bmkeg.digitalLibrary.dao;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.isi.bmkeg.digitalLibrary.model.citations.ArticleCitation;
import edu.isi.bmkeg.digitalLibrary.model.citations.Corpus;
import edu.isi.bmkeg.digitalLibrary.model.citations.Journal;
import edu.isi.bmkeg.digitalLibrary.model.citations.LiteratureCitation;
import edu.isi.bmkeg.ftd.model.FTD;
import edu.isi.bmkeg.lapdf.model.LapdfDocument;

import edu.isi.bmkeg.vpdmf.dao.CoreDao;
import edu.isi.bmkeg.vpdmf.model.instances.LightViewInstance;

/**
 * Defines the interface to a Data Access Object that manage the 
 * data persistent storage.
 * A Spring bean implementing this interface can be injected in other Spring beans,
 * like the ArticleServiceImpl bean.
 *
 */
public interface CitationsDao {

	
	public void setCoreDao(CoreDao coreDao);

	public CoreDao getCoreDao();

	
	// ~~~~~~~~~~~~~~~
	// Count functions
	// ~~~~~~~~~~~~~~~

	public int countCitations() throws Exception;
	
	public int countArticles() throws Exception;

	public int countCorpusArticles(String corpusName) throws Exception;

	// ~~~~~~~~~~~~~~~~~~~
	// Insert Functions
	// ~~~~~~~~~~~~~~~~~~~
	
	public void insertArticleCitation(ArticleCitation article) throws Exception;
	
	public void insertJournal(Journal journal) throws Exception;
	
	public void insertCorpus(Corpus corpus) throws Exception;

	void insertArticleCorpus(Corpus corpus) throws Exception;

	// ~~~~~~~~~~~~~~~~~~~
	// Update Functions
	// ~~~~~~~~~~~~~~~~~~~
	
	public void updateArticleCitation(ArticleCitation article) throws Exception;

	public void updateJournal(Journal j1) throws Exception ;

	public void updateCorpus(Corpus corpus) throws Exception;
	
	// ~~~~~~~~~~~~~~~~~~~
	// Delete Functions
	// ~~~~~~~~~~~~~~~~~~~
	
	public void deleteArticleCitation(long id) throws Exception;

	public void deleteCorpus(Corpus corpus) throws Exception;
	
	// ~~~~~~~~~~~~~~~~~~~~
	// Find by id Functions
	// ~~~~~~~~~~~~~~~~~~~~
	
	public ArticleCitation findArticleByVpdmfId(long id) throws Exception;
	
	public ArticleCitation findArticleByPmid(int pmid) throws Exception;
	
	public ArticleCitation findArticleById(String idCode, Integer id) throws Exception;

	public Journal findJournalById(long j1Id) throws Exception ;

	public Journal findJournalByAbbr(String abbr) throws Exception;
	
	public Corpus findCorpusById(long id) throws Exception;
	
	public Corpus findCorpusByName(String name) throws Exception;
	
	public Map<Integer, Long> lookupPmids(Set<Integer> keySet) throws Exception;
	
	public FTD findArticleDocumentByPmid(Integer pmid) throws Exception;
	
	public FTD findArticleDocumentById(String idCode, Integer id) throws Exception;
	
	// ~~~~~~~~~~~~~~~~~~~~
	// check Functions
	// ~~~~~~~~~~~~~~~~~~~~
	public boolean checkArticleByPmid(int pmid) throws Exception;
	
	// ~~~~~~~~~~~~~~~~~~~~
	// Retrieve functions
	// ~~~~~~~~~~~~~~~~~~~~

	//	It should look for all Publications, not just the Articles subset.
	public List<ArticleCitation> retrieveAllArticles() throws Exception;
	
	public List<ArticleCitation> retrieveAllArticlesPaged(int offset, int cnt) throws Exception;

	public List<ArticleCitation> retrieveCorpusArticlesPaged(String corpusName, int offset, int pageSize) throws Exception;

	public List<Journal> retrieveAllJournalsPaged(int offset, int cnt) throws Exception;

	// ~~~~~~~~~~~~~~
	// List functions
	// ~~~~~~~~~~~~~~
	
	public List<LightViewInstance> listAllJournalsPaged(int offset, int cnt) throws Exception;

	public List<LightViewInstance> listMatchingJournalsAbbrPaged(String abbrevPattern, int offset, int cnt) throws Exception;
	
	public List<LightViewInstance> listAllCorporaPaged(int offset, int pageSize)  throws Exception;

	public List<LightViewInstance> listAllCitationsPaged(int offset, int cnt) throws Exception;

	public List<LightViewInstance> listAllArticlesPaged(int offset, int cnt) throws Exception;

	public List<LightViewInstance> listCorpusArticles(String corpusName) throws Exception;
	
	public Map<Integer, Long> listAllPmidsPaged(int offset, int pageSize) throws Exception;

	// ~~~~~~~~~~~~~~~~~~~~
	// Add x to y functions
	// ~~~~~~~~~~~~~~~~~~~~

	public void addCorpusToArticle(long articleBmkegId, long corpusBmkegIdId) throws Exception;
	
	public void addCorpusToArticles(long corpusBmkegId, long[] articlesBmkegIds) throws Exception;
	
	public void addArticlesToCorpus(Set<Integer> keySet, String corpusName) throws Exception;
	
	public long addPdfToArticleCitation(LapdfDocument doc, ArticleCitation ac, 
			File pdf, String text) throws Exception;
	
	public void addSwfToFtd(File pdf, FTD ftd) throws Exception, IOException;

	// ~~~~~~~~~~~~~~~~~~~~~~~~~
	// Remove x from y functions
	// ~~~~~~~~~~~~~~~~~~~~~~~~~

	public void removeCorpusFromCitation(Corpus c, LiteratureCitation a);
	
}