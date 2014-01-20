package edu.isi.bmkeg.digitalLibrary.dao;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.isi.bmkeg.digitalLibrary.model.citations.ArticleCitation;
import edu.isi.bmkeg.digitalLibrary.model.citations.Corpus;
import edu.isi.bmkeg.digitalLibrary.model.citations.Journal;
import edu.isi.bmkeg.digitalLibrary.model.citations.JournalEpoch;
import edu.isi.bmkeg.digitalLibrary.model.citations.LiteratureCitation;
import edu.isi.bmkeg.ftd.model.FTD;
import edu.isi.bmkeg.ftd.model.FTDFragmentBlock;
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
public interface ExtendedDigitalLibraryDao {
	
	public void setCoreDao(CoreDao coreDao);

	public CoreDao getCoreDao();

	// ~~~~~~~~~~~~~~~~~~~~~
	// Convenience Functions
	// ~~~~~~~~~~~~~~~~~~~~~

	public Map<Integer, Long> lookupPmidsInTrans(Set<Integer> keySet) throws Exception;

	public ArticleCitation findArticleByIdInTrans(String idCode, Integer id) throws Exception;

	public ArticleCitation findArticleByPmidInTrans(Integer pmid) throws Exception;

	public Corpus findCorpusByNameInTrans(String name) throws Exception;
	
	public FTD findArticleDocumentByPmidInTrans(Integer pmid) throws Exception;
	
	public FTD findArticleDocumentByIdInTrans(String idCode, Integer id) throws Exception;
	
	// ~~~~~~~~~~~~~~~~~~~
	// Delete Functions
	// ~~~~~~~~~~~~~~~~~~~
	
	boolean fullyDeleteArticle(Long articleId) throws Exception;		
	
	// ~~~~~~~~~~~~~~~~~~~~
	// Add x to y functions
	// ~~~~~~~~~~~~~~~~~~~~
	public void addCorpusToArticle(long articleBmkegId, long corpusBmkegIdId) throws Exception;
	
	public void addCorpusToArticles(long corpusBmkegId, long[] articlesBmkegIds) throws Exception;
	
	public int addArticlesToCorpusWithIds(List<Long> articleIds, long corpusId) throws Exception;
	
	public void addArticlesToCorpus(Set<Integer> keySet, String corpusName) throws Exception;
	
	public long addPdfToArticleCitation(LapdfDocument doc, 
			ArticleCitation ac, 
			File pdf,
			File ruleFile) throws Exception;
	
	public void addSwfToFtd(File pdf, FTD ftd) throws Exception, IOException;

	// ~~~~~~~~~~~~~~~~~~~~~~~~~
	// Remove x from y functions
	// ~~~~~~~~~~~~~~~~~~~~~~~~~
	
	public boolean removeFragmentBlock(FTDFragmentBlock frgBlk) throws Exception;

	public int removeArticlesFromCorpusWithIds(List<Long> articleIds, long corpusId) throws Exception;



}