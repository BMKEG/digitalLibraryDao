package edu.isi.bmkeg.digitalLibrary.services;

import java.util.List;

import org.w3c.dom.Document;

import edu.isi.bmkeg.digitalLibrary.model.citations.ArticleCitation;
import edu.isi.bmkeg.digitalLibrary.model.qo.citations.ArticleCitation_qo;
import edu.isi.bmkeg.ftd.model.FTDFragmentBlock;
import edu.isi.bmkeg.ftd.model.FTDRuleSet;
import edu.isi.bmkeg.vpdmf.model.instances.LightViewInstance;

public interface ExtendedDigitalLibraryService {

	ArticleCitation addPmidEncodedPdfToCorpus(byte[] pdfFileData, 
			String fileName, String corpusName) throws Exception;

	boolean removeFragmentBlock(FTDFragmentBlock frgBlk) throws Exception;
	
	List<String> listTermViews() throws Exception;

	int addArticlesToCorpus(List<Long> articleIds, Long corpusId) throws Exception;
	
	int removeArticlesFromCorpus(List<Long> articleIds, Long corpusId) throws Exception;

	boolean fullyDeleteArticle(Long articleId) throws Exception;
	
	List<LightViewInstance> listExtendedJournalEpochs() throws Exception;
	
	Long addRuleFileToJournalEpoch(
			Long ruleFileId, 
			Long epochId,
			String epochJournal,
			int epochStart,
			int epochEnd) throws Exception;

	FTDRuleSet retrieveFTDRuleSetForArticleCitation(Long articleId) throws Exception;
	
	Long runRuleSetOnArticleCitation(Long ruleSetId, Long articleId) throws Exception;
	
	Long runRuleSetOnJournalEpoch(Long epochId) throws Exception;

	void runRulesOverAllEpochs() throws Exception;
	
	String generateRuleFileFromLapdf(Long articleId) throws Exception;
	
	byte[] loadSwf(Long vpdmfId) throws Exception;

	String loadXml(Long vpdmfId) throws Exception;

	String loadPmcXml(Long vpdmfId) throws Exception;
	
	String loadHtml(Long vpdmfId) throws Exception;
	
	List<LightViewInstance> listArticleCitationPaged(ArticleCitation_qo o, int offset, int cnt) throws Exception;

	String dumpFragmentsToBrat(long ftdId) throws Exception;
	
	Document retrieveFragmentTree(Long vpdmfId) throws Exception;

	byte[] packageCorpusArchive(Long corpusId) throws Exception;

	void cleanUpEmptyFragments() throws Exception;

}