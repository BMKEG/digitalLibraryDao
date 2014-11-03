package edu.isi.bmkeg.digitalLibrary.dao;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.transform.TransformerException;

import edu.isi.bmkeg.digitalLibrary.model.citations.ArticleCitation;
import edu.isi.bmkeg.digitalLibrary.model.citations.Corpus;
import edu.isi.bmkeg.digitalLibrary.model.citations.JournalEpoch;
import edu.isi.bmkeg.ftd.model.FTD;
import edu.isi.bmkeg.ftd.model.FTDFragmentBlock;
import edu.isi.bmkeg.ftd.model.FTDRuleSet;
import edu.isi.bmkeg.lapdf.model.LapdfDocument;
import edu.isi.bmkeg.vpdmf.dao.CoreDao;

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

	public Map<Integer, Long> lookupPmidsInTrans(Collection<Integer> keySet) throws Exception;

	public Map<Integer, Long> listAllPmidsInTrans() throws Exception;
	
	public ArticleCitation findArticleByIdInTrans(String idCode, Integer id) throws Exception;

	public ArticleCitation findArticleByPmidInTrans(Integer pmid) throws Exception;

	public Corpus findCorpusByNameInTrans(String name) throws Exception;
	
	public FTD findArticleDocumentByPmidInTrans(Integer pmid) throws Exception;
	
	public FTD findArticleDocumentByIdInTrans(String idCode, Integer id) throws Exception;

	public JournalEpoch retriveJournalEpochForCitation(ArticleCitation ac) throws Exception;
	
	public FTDRuleSet readRuleFileFromDisk(File ruleFile) throws Exception;
	
	
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
	
	public int addArticlesToCorpusWithIdsInTrans(List<Long> articleIds, long corpusId) throws Exception;

	public void addArticlesToCorpus(List<Integer> keySet, String corpusName) throws Exception;
	
	public void addArticlesToCorpusInTrans(List<Integer> pmids, String corpusName) throws Exception;

	//public long addFtdToArticleCitation(ArticleCitation ac) throws Exception;
	
	public long addFtdToArticleCitation(LapdfDocument doc, 
			ArticleCitation ac, 
			File pdf) throws Exception;
	
	public String addSwfToFtd(File pdf, FTD ftd) throws Exception, IOException;

	// ~~~~~~~~~~~~~~~~~~~~~~~~~
	// Remove x from y functions
	// ~~~~~~~~~~~~~~~~~~~~~~~~~
	
	public boolean removeFragmentBlock(FTDFragmentBlock frgBlk) throws Exception;

	public int removeArticlesFromCorpusWithIds(List<Long> articleIds, long corpusId) throws Exception;

	// ~~~~~~~~~~~~~~~~~~~~~~~~~
	// Higher-levels functions
	// ~~~~~~~~~~~~~~~~~~~~~~~~~
	
	public String retrieveTextFromFtd(FTD ftd) throws SQLException, IOException, 
			TransformerException, FileNotFoundException;


}