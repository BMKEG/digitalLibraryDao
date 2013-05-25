package edu.isi.bmkeg.digitalLibrary.dao.vpdmf;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.prefs.Preferences;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import edu.isi.bmkeg.digitalLibrary.dao.CitationsDao;
import edu.isi.bmkeg.digitalLibrary.model.citations.ArticleCitation;
import edu.isi.bmkeg.digitalLibrary.model.citations.Corpus;
import edu.isi.bmkeg.digitalLibrary.model.citations.Journal;
import edu.isi.bmkeg.digitalLibrary.model.citations.LiteratureCitation;
import edu.isi.bmkeg.ftd.model.FTD;
import edu.isi.bmkeg.lapdf.model.LapdfDocument;
import edu.isi.bmkeg.uml.model.UMLclass;
import edu.isi.bmkeg.utils.Converters;
import edu.isi.bmkeg.vpdmf.controller.queryEngineTools.ChangeEngine;
import edu.isi.bmkeg.vpdmf.controller.queryEngineTools.VPDMfChangeEngineInterface;
import edu.isi.bmkeg.vpdmf.dao.CoreDao;
import edu.isi.bmkeg.vpdmf.model.definitions.PrimitiveDefinition;
import edu.isi.bmkeg.vpdmf.model.definitions.PrimitiveLink;
import edu.isi.bmkeg.vpdmf.model.definitions.VPDMf;
import edu.isi.bmkeg.vpdmf.model.definitions.ViewDefinition;
import edu.isi.bmkeg.vpdmf.model.instances.AttributeInstance;
import edu.isi.bmkeg.vpdmf.model.instances.ClassInstance;
import edu.isi.bmkeg.vpdmf.model.instances.LightViewInstance;
import edu.isi.bmkeg.vpdmf.model.instances.PrimitiveInstance;
import edu.isi.bmkeg.vpdmf.model.instances.ViewBasedObjectGraph;
import edu.isi.bmkeg.vpdmf.model.instances.ViewInstance;

@Repository
public class VpdmfCitationsDao implements CitationsDao {

	private static Logger logger = Logger.getLogger(VpdmfCitationsDao.class);

	// ~~~~~~~~~
	// Constants
	// ~~~~~~~~~
	private static final String CITATION_VIEW_NAME = "LiteratureCitations";
	private static final String ARTICLE_VIEW_NAME = "ArticleCitation";
	private static final String JOURNAL_VIEW_NAME = "Journal";
	private static final String CORPUS_VIEW_NAME = "Corpus";

	@Autowired
	private CoreDao coreDao;

	// ~~~~~~~~~~~~
	// Constructors
	// ~~~~~~~~~~~~
	public VpdmfCitationsDao() throws Exception {
//		this.coreDao = new CoreDaoImpl();
	}

	public VpdmfCitationsDao(CoreDao coreDao) {
		this.coreDao = coreDao;
	}

	// ~~~~~~~~~~~~~~~~~~~
	// Getters and Setters
	// ~~~~~~~~~~~~~~~~~~~
	public void setCoreDao(CoreDao dlVpdmf) {
		this.coreDao = dlVpdmf;
	}

	public CoreDao getCoreDao() {
		return coreDao;
	}

	private VPDMfChangeEngineInterface getCe() {
		return coreDao.getCe();
	}

	private Map<String, ViewBasedObjectGraph> generateVbogs() throws Exception {
		return coreDao.generateVbogs();
	}

	private VPDMf getTop() {
		return coreDao.getTop();
	}

	// ~~~~~~~~~~~~~~~
	// Count functions
	// ~~~~~~~~~~~~~~~

	public int countCitations() throws Exception {

		return getCoreDao().countView(CITATION_VIEW_NAME);

	}

	public int countArticles() throws Exception {

		return getCoreDao().countView(ARTICLE_VIEW_NAME);

	}

	public int countCorpusArticles(String corpusName) throws Exception {

		try {

			getCe().connectToDB();
			getCe().turnOffAutoCommit();

			ViewDefinition vd = getTop().getViews().get(ARTICLE_VIEW_NAME);
//			ViewBasedObjectGraph vbog = generateVbogs().get(ARTICLE_VIEW_NAME);

			ViewInstance vi = new ViewInstance(vd);

			AttributeInstance ai = vi.readAttributeInstance(
					"]Corpus|Corpus.name", 0);
			ai.writeValueString(corpusName);

			return getCe().executeCountQuery(vi);
			
		} finally {
			getCe().closeDbConnection();
		}

	}

	// ~~~~~~~~~~~~~~~~~~~
	// Insert Functions
	// ~~~~~~~~~~~~~~~~~~~
	public void insertArticleCitation(ArticleCitation article) throws Exception {

		getCoreDao().insert(article, "ArticleCitation");

	}

	public void insertJournal(Journal journal) throws Exception {

		getCoreDao().insertVBOG(journal, "Journal");

	}

	public void insertCorpus(Corpus corpus) throws Exception {

		getCoreDao().insertVBOG(corpus, "Corpus");

	}

	public void insertArticleCorpus(Corpus corpus) throws Exception {

		getCoreDao().insertVBOG(corpus, "ArticleCorpus");

	}

	// ~~~~~~~~~~~~~~~~~~~
	// Update Functions
	// ~~~~~~~~~~~~~~~~~~~
	// TODO make this a generic method in the CoreDao class
	public void updateArticleCitation(ArticleCitation article) throws Exception {

		try {

			getCe().connectToDB();
			getCe().turnOffAutoCommit();

			ViewInstance vi0;
			try {
				vi0 = getCe().executeUIDQuery(ARTICLE_VIEW_NAME,
						article.getVpdmfId());
			} catch (Exception e) {
				throw new Exception(
						"No article with id: "
								+ article.getVpdmfId()
								+ " was found for updating. You might want to use insertArtcileCitation instead.");
			}

			getCe().storeViewInstanceForUpdate(vi0);

			ViewBasedObjectGraph vbog = generateVbogs().get(ARTICLE_VIEW_NAME);

			ViewInstance vi1 = vbog.objectGraphToView(article);
			Map<String, Object> objMap = vbog.getObjMap();

			getCe().executeUpdateQuery(vi1);

			Iterator<String> keyIt = objMap.keySet().iterator();
			while (keyIt.hasNext()) {
				String key = keyIt.next();
				PrimitiveInstance pi = (PrimitiveInstance) vi1.getSubGraph().getNodes().get(key);
				Object o = objMap.get(key);
				vbog.primitiveToObject(pi, o, true);
			}

			getCe().commitTransaction();

		} catch (Exception e) {

			getCe().rollbackTransaction();

			throw e;

		} finally {

			getCe().closeDbConnection();

		}
	}

	// TODO make this a generic method in the CoreDao class
	public void updateJournal(Journal journal) throws Exception {

		try {

			getCe().connectToDB();
			getCe().turnOffAutoCommit();

			ViewInstance vi0;
			try {
				vi0 = getCe().executeUIDQuery(JOURNAL_VIEW_NAME,
						journal.getVpdmfId());
			} catch (Exception e) {
				throw new Exception(
						"No journal with id: "
								+ journal.getVpdmfId()
								+ " was found for updating. You might want to use insertJournal instead.");
			}

			getCe().storeViewInstanceForUpdate(vi0);

			ViewBasedObjectGraph vbog = generateVbogs().get(JOURNAL_VIEW_NAME);

			ViewInstance vi1 = vbog.objectGraphToView(journal);
			Map<String, Object> objMap = vbog.getObjMap();

			getCe().executeUpdateQuery(vi1);

			Iterator<String> keyIt = objMap.keySet().iterator();
			while (keyIt.hasNext()) {
				String key = keyIt.next();
				PrimitiveInstance pi = (PrimitiveInstance) vi1.getSubGraph()
						.getNodes().get(key);
				Object o = objMap.get(key);
				vbog.primitiveToObject(pi, o, true);
			}

			getCe().commitTransaction();

		} catch (Exception e) {

			getCe().rollbackTransaction();

			throw e;

		} finally {

			getCe().closeDbConnection();

		}

	}

	// TODO make this a generic method in the CoreDao class
	public void updateCorpus(Corpus corpus) throws Exception {

		try {

			getCe().connectToDB();
			getCe().turnOffAutoCommit();

			ViewInstance vi0;
			try {
				vi0 = getCe().executeUIDQuery(CORPUS_VIEW_NAME,
						corpus.getVpdmfId());
			} catch (Exception e) {
				throw new Exception("No corpus with id: " + corpus.getVpdmfId()
						+ " was found for updating. "
						+ " You may need to insert this Corpus first.");
			}

			getCe().storeViewInstanceForUpdate(vi0);

			ViewBasedObjectGraph vbog = generateVbogs().get(CORPUS_VIEW_NAME);

			ViewInstance vi1 = vbog.objectGraphToView(corpus);
			Map<String, Object> objMap = vbog.getObjMap();

			getCe().executeUpdateQuery(vi1);

			Iterator<String> keyIt = objMap.keySet().iterator();
			while (keyIt.hasNext()) {
				String key = keyIt.next();
				PrimitiveInstance pi = (PrimitiveInstance) vi1.getSubGraph()
						.getNodes().get(key);
				Object o = objMap.get(key);
				vbog.primitiveToObject(pi, o, true);
			}

			getCe().commitTransaction();

		} catch (Exception e) {

			getCe().rollbackTransaction();

			throw e;

		} finally {

			getCe().closeDbConnection();

		}

	}

	// ~~~~~~~~~~~~~~~~~~~~~~~~~
	// Remove x from y functions
	// ~~~~~~~~~~~~~~~~~~~~~~~~~
	public void removeCorpusFromCitation(Corpus c, LiteratureCitation a) {

	}

	// ~~~~~~~~~~~~~~~~~~~
	// Delete Functions
	// ~~~~~~~~~~~~~~~~~~~
	public void deleteArticleCitation(long id) throws Exception {

		try {

			getCe().connectToDB();
			getCe().turnOffAutoCommit();

			getCe().deleteView(ARTICLE_VIEW_NAME, id );

		} catch (Exception e) {

			throw e;

		} finally {

			getCe().closeDbConnection();

		}

	}

	
	public void deleteCorpus(Corpus corpus) throws Exception {

		try {

			getCe().connectToDB();
			getCe().turnOffAutoCommit();

			getCe().deleteView(CORPUS_VIEW_NAME, corpus.getVpdmfId() );

		} catch (Exception e) {

			throw e;

		} finally {

			getCe().closeDbConnection();

		}

		// TODO Auto-generated method stub
		
	}
	
	// ~~~~~~~~~~~~~~~~~~~~
	// Find by id Functions
	// ~~~~~~~~~~~~~~~~~~~~
	
	public ArticleCitation findArticleByVpdmfId(long id) throws Exception {

		ArticleCitation a = (ArticleCitation) getCoreDao().findVBOGById(id,
				ARTICLE_VIEW_NAME);
		// HACK to get around viewToObjectGraph() bug
		// TODO: GULLY: Please fix this
		if (a.getCorpora().size() == 1
				&& a.getCorpora().get(0).getVpdmfId() == 0)
			a.getCorpora().clear();

		return a;

	}

	public ArticleCitation findArticleByPmid(Integer pmid) throws Exception {

		ArticleCitation a = (ArticleCitation) getCoreDao()
				.findVBOGByAttributeValue("ArticleCitation",
						"LiteratureCitation", "ArticleCitation", "pmid",
						pmid + "");

		if (a == null)
			return null;

		// HACK to get around viewToObjectGraph() bug
		// TODO: GULLY: Please fix this
		if (a.getCorpora() != null && a.getCorpora().size() == 1
				&& a.getCorpora().get(0).getVpdmfId() == 0)
			a.getCorpora().clear();

		return a;

	}

	public ArticleCitation findArticleByPmid(int pmid) throws Exception {

		return (ArticleCitation) getCoreDao().findVBOGByAttributeValue(
				ARTICLE_VIEW_NAME, "LiteratureCitation", "ArticleCitation",
				"pmid", String.valueOf(pmid));
	}
	
	public ArticleCitation findArticleById(String idCode, Integer id) throws Exception {

		getCe().connectToDB();
		getCe().turnOffAutoCommit();

		ViewDefinition vd = getTop().getViews().get(ARTICLE_VIEW_NAME);
		ClassLoader cl = VpdmfCitationsDao.class.getClassLoader();
		ViewBasedObjectGraph vbog = new ViewBasedObjectGraph(getTop(), cl, ARTICLE_VIEW_NAME);

		ViewInstance vi = new ViewInstance(vd);
		AttributeInstance aiIdType = vi.readAttributeInstance("]ID|ID.type", 0);
		aiIdType.setValue(idCode);
		AttributeInstance aiIdValue = vi.readAttributeInstance("]ID|ID.value", 0);
		aiIdValue.setValue(id);
		
		List<LightViewInstance> lvil = getCe().executeListQuery(vi);
		if( lvil.size() > 1 ) {
			throw new Exception( idCode + ":" + id + " ambiguous, more than one " + 
					"article citation returned");
		} else if( lvil.size() == 0 ) {
			return null;
		}
		
		LightViewInstance lvi = lvil.get(0);
		ViewInstance artVi = getCe().executeUIDQuery(lvi);
		
		vbog.viewToObjectGraph(artVi);
		ArticleCitation art = (ArticleCitation) vbog.readPrimaryObject();

		return art;

	}

	// TODO refactor to use generic method from CoreDao
	public List<ArticleCitation> retrieveAllArticles() throws Exception {

		try {

			getCe().connectToDB();
			getCe().turnOffAutoCommit();

			ViewDefinition vd = getTop().getViews().get(ARTICLE_VIEW_NAME);
			ViewBasedObjectGraph vbog = generateVbogs().get(ARTICLE_VIEW_NAME);

			ViewInstance vi = new ViewInstance(vd);

			List<ArticleCitation> l = new ArrayList<ArticleCitation>();
			Iterator<ViewInstance> it = getCe().executeFullQuery(vi).iterator();
			while (it.hasNext()) {
				ViewInstance lvi = it.next();

				vbog.viewToObjectGraph(lvi);
				ArticleCitation a = (ArticleCitation) vbog.readPrimaryObject();

				l.add(a);

			}

			return l;

		} finally {
			getCe().closeDbConnection();
		}

	}

	public Journal findJournalById(long id) throws Exception {

		return (Journal) getCoreDao().findVBOGById(id, JOURNAL_VIEW_NAME);

	}

	// TODO refactor to use generic method from CoreDao
	public Journal findJournalByAbbr(String abbr) throws Exception {

		try {

			getCe().connectToDB();
			getCe().turnOffAutoCommit();

			ViewDefinition vd = getTop().getViews().get(JOURNAL_VIEW_NAME);
			ViewBasedObjectGraph vbog = generateVbogs().get(JOURNAL_VIEW_NAME);

			ViewInstance vi = new ViewInstance(vd);

			AttributeInstance ai = vi.readAttributeInstance(
					"]Journal|Journal.abbr", 0);
			ai.writeValueString(abbr);

			Journal j = null;

			try {
				List<ViewInstance> l = getCe().executeFullQuery(vi, true, 0, 1);
				if (l != null) {
					Iterator<ViewInstance> it = l.iterator();
					if (it.hasNext()) {
						ViewInstance lvi = l.iterator().next();
						vbog.viewToObjectGraph(lvi);
						j = (Journal) vbog.readPrimaryObject();
					}
				}
			} catch (Exception e) {
			}

			return j;

		} finally {
			getCe().closeDbConnection();
		}

	}

	public Corpus findCorpusById(long id) throws Exception {

		return (Corpus) getCoreDao().findVBOGById(id, CORPUS_VIEW_NAME);

	}

	public Corpus findCorpusByName(String name) throws Exception {

		return (Corpus) getCoreDao().findVBOGByAttributeValue(CORPUS_VIEW_NAME,
				"Corpus", "Corpus", "name", name);

	}
	
	public FTD findArticleDocumentByPmid(Integer pmid) throws Exception {

		getCe().connectToDB();
		getCe().turnOffAutoCommit();

		ViewDefinition vd = getTop().getViews().get("ArticleDocument");
		ClassLoader cl = VpdmfCitationsDao.class.getClassLoader();
		ViewBasedObjectGraph vbog = new ViewBasedObjectGraph(getTop(), cl, "ArticleDocument");

		ViewInstance vi = new ViewInstance(vd);
		AttributeInstance aiIdType = vi.readAttributeInstance("]LiteratureCitation|ArticleCitation.pmid", 0);
		aiIdType.setValue(pmid);
		
		List<LightViewInstance> lvil = getCe().executeListQuery(vi);
		if( lvil.size() > 1 ) {
			throw new Exception( "pmid:" + pmid + " ambiguous, more than one " + 
					"article citation returned");
		} else if( lvil.size() == 0 ) {
			return null;
		}
		
		LightViewInstance lvi = lvil.get(0);
		ViewInstance artVi = getCe().executeUIDQuery(lvi);
		
		vbog.viewToObjectGraph(artVi);
		FTD ftd = (FTD) vbog.readPrimaryObject();

		return ftd;

	}
	
	public FTD findArticleDocumentById(String idCode, Integer id) throws Exception {

		getCe().connectToDB();
		getCe().turnOffAutoCommit();

		ViewDefinition vd = getTop().getViews().get("ArticleDocument");
		ClassLoader cl = VpdmfCitationsDao.class.getClassLoader();
		ViewBasedObjectGraph vbog = new ViewBasedObjectGraph(getTop(), cl, "ArticleDocument");

		ViewInstance vi = new ViewInstance(vd);
		AttributeInstance aiIdType = vi.readAttributeInstance("]ID|ID.type", 0);
		aiIdType.setValue(idCode);
		AttributeInstance aiIdValue = vi.readAttributeInstance("]ID|ID.value", 0);
		aiIdValue.setValue(id);
		
		List<LightViewInstance> lvil = getCe().executeListQuery(vi);
		if( lvil.size() > 1 ) {
			throw new Exception( idCode + ":" + id + " ambiguous, more than one " + 
					"article citation returned");
		} else if( lvil.size() == 0 ) {
			return null;
		}
		
		LightViewInstance lvi = lvil.get(0);
		ViewInstance ftdVi = getCe().executeUIDQuery(lvi);
		
		vbog.viewToObjectGraph(ftdVi);
		FTD ftd = (FTD) vbog.readPrimaryObject();

		return ftd;

	}
	
	// ~~~~~~~~~~~~~~~~~~~~
	// check Functions
	// ~~~~~~~~~~~~~~~~~~~~
	/**
	 * Fastest possible check to see if Article in database;
	 */
	public boolean checkArticleByPmid(int pmid) throws Exception {
		
return false;
		
		
	}
	
	
	// ~~~~~~~~~~~~~~~~~~~~
	// Retrieve functions
	// ~~~~~~~~~~~~~~~~~~~~

	// TODO refactor to use generic method from CoreDao
	public List<ArticleCitation> retrieveAllArticlesPaged(int offset,
			int pageSize) throws Exception {
		
		try {

			getCe().connectToDB();
			getCe().turnOffAutoCommit();

			ViewDefinition vd = getTop().getViews().get(ARTICLE_VIEW_NAME);
			ViewBasedObjectGraph vbog = generateVbogs().get(ARTICLE_VIEW_NAME);

			ViewInstance vi = new ViewInstance(vd);

			List<ArticleCitation> l = new ArrayList<ArticleCitation>();

			Iterator<ViewInstance> it = getCe().executeFullQuery(vi, true,
					offset, pageSize).iterator();
			while (it.hasNext()) {
				ViewInstance lvi = it.next();

				vbog.viewToObjectGraph(lvi);
				Object o = vbog.readPrimaryObject();
				ArticleCitation a = (ArticleCitation) o;

				l.add(a);

			}

			return l;

		} catch (Exception e) {
			e.printStackTrace();
			return null;

		} finally {
			getCe().closeDbConnection();
		}
	}

	// TODO refactor to use generic method from CoreDao
	public List<ArticleCitation> retrieveCorpusArticlesPaged(String corpusName,
			int offset, int pageSize) throws Exception {
		
		try {

			getCe().connectToDB();
			getCe().turnOffAutoCommit();

			ViewDefinition vd = getTop().getViews().get(ARTICLE_VIEW_NAME);
			ViewBasedObjectGraph vbog = generateVbogs().get(ARTICLE_VIEW_NAME);

			ViewInstance vi = new ViewInstance(vd);

			AttributeInstance ai = vi.readAttributeInstance(
					"]Corpus|Corpus.name", 0);
			ai.writeValueString(corpusName);

			List<ArticleCitation> l = new ArrayList<ArticleCitation>();

			Iterator<ViewInstance> it = getCe().executeFullQuery(vi, true,
					offset, pageSize).iterator();
			while (it.hasNext()) {
				ViewInstance lvi = it.next();

				vbog.viewToObjectGraph(lvi);
				Object o = vbog.readPrimaryObject();
				ArticleCitation a = (ArticleCitation) o;

				l.add(a);

			}

			return l;

		} finally {

			getCe().closeDbConnection();
		
		}
		
	}

	// TODO refactor to use generic method from CoreDao
	public List<Journal> retrieveAllJournalsPaged(int offset, int pageSize)
			throws Exception {
		try {

			getCe().connectToDB();
			getCe().turnOffAutoCommit();

			ViewDefinition vd = getTop().getViews().get(JOURNAL_VIEW_NAME);
			ViewBasedObjectGraph vbog = generateVbogs().get(JOURNAL_VIEW_NAME);

			ViewInstance vi = new ViewInstance(vd);

			List<Journal> l = new ArrayList<Journal>();
			Iterator<ViewInstance> it = getCe().executeFullQuery(vi, true,
					offset, pageSize).iterator();
			while (it.hasNext()) {
				ViewInstance lvi = it.next();

				vbog.viewToObjectGraph(lvi);
				Journal j = (Journal) vbog.readPrimaryObject();

				l.add(j);

			}

			return l;

		} finally {
			getCe().closeDbConnection();
		}
	}

	// ~~~~~~~~~~~~~~
	// List functions
	// ~~~~~~~~~~~~~~

	// ADDED BY GULLY
	public List<LightViewInstance> listAllCitationsPaged(int offset,
			int pageSize) throws Exception {

		try {

			getCe().connectToDB();
			getCe().turnOffAutoCommit();

			ViewDefinition vd = getTop().getViews().get(CITATION_VIEW_NAME);

			ViewInstance vi = new ViewInstance(vd);

			List<LightViewInstance> l = new ArrayList<LightViewInstance>();
			Iterator<LightViewInstance> it = getCe().executeListQuery(vi, true,
					offset, pageSize).iterator();
			while (it.hasNext()) {
				LightViewInstance lvi = it.next();
				l.add(lvi);
			}

			return l;

		} finally {

			getCe().closeDbConnection();

		}

	}

	// ADDED BY GULLY
	public List<LightViewInstance> listAllArticlesPaged(int offset, int pageSize)
			throws Exception {

		try {

			getCe().connectToDB();
			getCe().turnOffAutoCommit();

			ViewDefinition vd = getTop().getViews().get(ARTICLE_VIEW_NAME);

			ViewInstance vi = new ViewInstance(vd);

			List<LightViewInstance> l = new ArrayList<LightViewInstance>();
			Iterator<LightViewInstance> it = getCe().executeListQuery(vi, true,
					offset, pageSize).iterator();
			while (it.hasNext()) {
				LightViewInstance lvi = it.next();
				l.add(lvi);
			}

			return l;

		} finally {

			getCe().closeDbConnection();

		}

	}

	public List<LightViewInstance> listAllJournalsPaged(int offset, int pageSize)
			throws Exception {
		try {

			getCe().connectToDB();
			getCe().turnOffAutoCommit();

			ViewDefinition vd = getTop().getViews().get(JOURNAL_VIEW_NAME);

			ViewInstance vi = new ViewInstance(vd);

			List<LightViewInstance> l = new ArrayList<LightViewInstance>();
			Iterator<LightViewInstance> it = getCe().executeListQuery(vi, true,
					offset, pageSize).iterator();
			while (it.hasNext()) {
				LightViewInstance lvi = it.next();
				l.add(lvi);
			}

			return l;

		} finally {
			getCe().closeDbConnection();
		}
	}

	public List<LightViewInstance> listMatchingJournalsAbbrPaged(
			String abbrevPattern, int offset, int pageSize) throws Exception {

		try {

			getCe().connectToDB();
			getCe().turnOffAutoCommit();

			ViewDefinition vd = getTop().getViews().get(JOURNAL_VIEW_NAME);

			ViewInstance vi = new ViewInstance(vd);

			AttributeInstance ai = vi.readAttributeInstance(
					"]Journal|Journal.abbr", 0);
			ai.writeValueString(abbrevPattern);

			List<LightViewInstance> l = new ArrayList<LightViewInstance>();
			Iterator<LightViewInstance> it = getCe().executeListQuery(vi, true,
					offset, pageSize).iterator();
			while (it.hasNext()) {
				LightViewInstance lvi = it.next();
				l.add(lvi);
			}

			return l;

		} finally {
			getCe().closeDbConnection();
		}
	}

	public List<LightViewInstance> listAllCorporaPaged(int offset, int pageSize)
			throws Exception {
		try {

			getCe().connectToDB();
			getCe().turnOffAutoCommit();

			ViewDefinition vd = getTop().getViews().get(CORPUS_VIEW_NAME);

			ViewInstance vi = new ViewInstance(vd);

			List<LightViewInstance> l = new ArrayList<LightViewInstance>();
			Iterator<LightViewInstance> it = getCe().executeListQuery(vi, true,
					offset, pageSize).iterator();
			while (it.hasNext()) {
				LightViewInstance lvi = it.next();
				l.add(lvi);
			}

			return l;

		} finally {
			getCe().closeDbConnection();
		}
	}

	// ADDED BY GULLY
	public Map<Integer, Long> listAllPmidsPaged(int offset, int pageSize)
			throws Exception {

		Map<Integer, Long> pmids = new HashMap<Integer, Long>();

		try {

			getCe().connectToDB();
			getCe().turnOffAutoCommit();

			ViewDefinition vd = getTop().getViews().get(ARTICLE_VIEW_NAME);
			String pName = "LiteratureCitation";
			String cName = "ArticleCitation";
			String aName = "pmid";

			ViewInstance vi = new ViewInstance(vd);

			Map<Long, Object> m = getCe().executeAttributeQuery(vi, pName,
					cName, aName, true, offset, pageSize);
			Iterator<Long> it = m.keySet().iterator();
			while (it.hasNext()) {
				Long vpdmfId = it.next();

				Integer pmid = (Integer) m.get(vpdmfId);

				if (pmids.containsKey(pmid)) {
					logger.info("Duplicate article in database: " + pmid);
				}

				pmids.put(pmid, vpdmfId);

			}

		} finally {

			getCe().closeDbConnection();

		}

		return pmids;

	}
	
	public List<LightViewInstance> listCorpusArticles(String corpusName) throws Exception {

		List<LightViewInstance> l = null;

		try{
		
			getCe().connectToDB();
	
			l = this.coreDao.goGetLightViewList("ArticleCitation", 
					"]Corpus|Corpus.name", corpusName);
			
		} finally {
	
			getCe().closeDbConnection();
	
		}
		
		return l;
		
	}

	// ADDED BY GULLY - KINDA HORRIBLE
	public Map<Integer, Long> lookupPmids(Set<Integer> pmids)
			throws Exception {

		Map<Integer, Long> pmidMap = new HashMap<Integer, Long>();

		try {

			getCe().connectToDB();
			getCe().turnOffAutoCommit();

			VPDMf top = getCe().readTop();
			Set<UMLclass> cc = top.getUmlModel().lookupClass("ArticleCitation");
			UMLclass c = cc.iterator().next();

			long t = System.currentTimeMillis();

			Iterator<Integer> it = pmids.iterator();
			while (it.hasNext()) {
				Integer pmid = it.next();

				// TODO: IMPROVE THIS HACK
				ClassInstance ci = new ClassInstance(c);

				AttributeInstance ai = ci.getAttributes().get("pmid");
				ai.setValue(pmid);

				List<ClassInstance> lci = getCe().queryClass(ci);

				if (lci.size() == 1) {
					ClassInstance ciRet = lci.get(0);
					AttributeInstance bmkegIdAi = ciRet.getAttributes().get(
							"vpdmfId");
					pmidMap.put(pmid, new Long(bmkegIdAi.readValueString()));
				} else if (lci.size() == 0) {
					logger.debug("Can't find pmmid: " + pmid);
				} else {
					logger.info("pmmid: " + pmid + " ambiguous ");
				}

			}

			long deltaT = System.currentTimeMillis() - t;
			logger.info("check for " + pmids.size()
					+ " entries in database in " + deltaT / 1000.0 + " s\n");

		} finally {

			getCe().closeDbConnection();

		}

		return pmidMap;

	}

	// ~~~~~~~~~~~~~~~~~~~~
	// Add x to y functions
	// ~~~~~~~~~~~~~~~~~~~~

	public long addPdfToArticleCitation(LapdfDocument doc,
			ArticleCitation ac, File pdf, String text) throws Exception {

		FTD ftd = new FTD();
		
		//
		// Here is where we run the pdf2Swf command.
		//
		addSwfToFtd(pdf, ftd);
		
		ftd.setChecksum(Converters.checksum(pdf));
		ftd.setName(pdf.getPath());
		ftd.setText(text);

		doc.packForSerialization();
		ftd.setLapdf(Converters.objectToByteArray(doc));
		doc.unpackFromSerialization();

		ftd.setCitation(ac);
		ac.setFullText(ftd);

		return this.getCoreDao().insertVBOG(ftd, "ArticleDocument");

	}

	public void addSwfToFtd(File pdf, FTD ftd) throws Exception, IOException {
		
		File swfBinDir = Converters.readAppDirectory("swftools");
		
		if( swfBinDir != null ) {

			String swfPath = swfBinDir + "/pdf2swf";
			if( System.getProperty("os.name").toLowerCase().contains("win") ) {
				swfPath += ".exe";
			}
			
			String pdfStem = pdf.getName().replaceAll("\\.pdf", "");
			File swfFile = new File( pdf.getParent() + "/" + pdfStem + ".swf" );
			
			Process p = Runtime.getRuntime().exec(swfPath + " " + pdf.getPath() 
					+ " -o " + swfFile.getPath());
			
			InputStream in = p.getInputStream();
			BufferedInputStream buf = new BufferedInputStream(in);
			InputStreamReader inread = new InputStreamReader(buf);
			BufferedReader bufferedreader = new BufferedReader(inread);
	        String line, out = "";
	        while ((line = bufferedreader.readLine()) != null) {
	        	out += line + "\n";
	        }
	        // Check for maven failure
	        try {
	        	if (p.waitFor() != 0) {
	        		out += "exit value = " + p.exitValue() + "\n";
	        	}
	        } catch (InterruptedException e) {
	        	out += "ERROR:\n" + e.getStackTrace().toString() + "\n";
	        } finally {
	        	// Close the InputStream
	        	bufferedreader.close();
	        	inread.close();
	        	buf.close();
	        	in.close();
			}
	        
			if( !swfFile.exists() ) {
				throw new Exception("pdf2swf-based swf generation failed: " + out );
			}
			
			ftd.setLaswf( Converters.fileContentsToBytesArray(swfFile) );
			
		}
		
	}


	public void addArticlesToCorpus(Set<Integer> keySet, String corpusName)
			throws Exception {
	
		this.addArticlesToCorpusUsingClasses(keySet, corpusName);
	
	}
	
	public void addArticlesToCorpusUsingClasses(Set<Integer> pmids,
			String corpusName) throws Exception {

		Corpus c = this.findCorpusByName(corpusName);
		if (c == null) {
  			throw new Exception("Could not find a corpus named: " + corpusName);
		}
		Long corpusId = c.getVpdmfId();

		ChangeEngine ce = (ChangeEngine) this.coreDao.getCe();
		VPDMf top = ce.readTop();
		ViewDefinition vd = top.getViews().get("ArticleCorpus");

		ViewInstance vi = new ViewInstance(vd);

		PrimitiveLink pl = (PrimitiveLink) vd.getSubGraph().getEdges()
				.iterator().next();
		UMLclass link = pl.getRole().getAss().getLinkClass();

		Map<Integer, Long> bmkegIdMap = lookupPmids(pmids);

		// We are going to write the data that we want to insert into the
		// database into
		// a local file and then insert that as a batch function.
		// - need to lock tables as we do this.
		ce.connectToDB();
		ce.turnOffAutoCommit();

		try {
			List<Integer> pmidList = new ArrayList<Integer>(pmids);
			Collections.sort(pmidList);
			Iterator<Integer> pmidIt = pmidList.iterator();
			while (pmidIt.hasNext()) {
				Integer pmid = pmidIt.next();
				Long articleId = bmkegIdMap.get(pmid);
				
				if( articleId == null )
					continue;

				ClassInstance linkCi = new ClassInstance(link);

				AttributeInstance corpusIdAi = linkCi.getAttributes().get(
						"corpora_id");
				corpusIdAi.setValue(corpusId);

				AttributeInstance articleIdAi = linkCi.getAttributes().get(
						"resources_id");
				articleIdAi.setValue(articleId);
				
				List<ClassInstance> l = ce.queryClass(linkCi);
				if( l.size() == 0 ) {
					ce.insertObjectIntoDB(linkCi);
				} 

			}

			ce.commitTransaction();

		} catch (Exception e) {
			
			throw e;

		} finally {

			this.coreDao.getCe().closeDbConnection();

		}

	}

	public void addArticlesToCorpusUsingViews(Set<Integer> pmids,
			String corpusName) throws Exception {

		try {

			int pgSz = 100;
			ChangeEngine ce = (ChangeEngine) this.coreDao.getCe();
			VPDMf top = ce.readTop();

			ViewDefinition corVd = top.getViews().get("ArticleCorpus");
			ViewInstance queryVi = new ViewInstance(corVd);

			ce.connectToDB();
			ce.turnOffAutoCommit();

			ViewInstance qVi = new ViewInstance(corVd);
			PrimitiveDefinition articlePd = (PrimitiveDefinition) 
					corVd.getSubGraph().getNodes().get("ArticleCitationLU");
			PrimitiveInstance corPi = qVi.getPrimaryPrimitive();
			AttributeInstance corAi = qVi.readAttributeInstance("Corpus",
					"Corpus", "name", 0);
			corAi.setValue(corpusName);

			List<LightViewInstance> corViList = ce.executeListQuery(qVi);
			if (corViList.size() == 0) {
				return;
			}

			List<ViewInstance> corpusList = ce.executeFullQuery(qVi);
			ViewInstance corVi = corpusList.get(0);
			
			ce.storeViewInstanceForUpdate(corVi);

			// Build a large view instance and populate it.
			int i = 0;
			Iterator<Integer> pmidIt = pmids.iterator();
			while (pmidIt.hasNext()) {
				Integer pmid = pmidIt.next();

				if (i >= corVi.countPrimitives(articlePd) )
					corVi.addNewPrimitiveInstance("ArticleCitationLU", i);

				PrimitiveInstance citPi = (PrimitiveInstance) corVi
						.getSubGraph().getNodes().get("AritcleCitationLU_" + i);
				AttributeInstance citAi = corVi.readAttributeInstance(
						"ArticleCitationLU", "ArticleCitation", "pmid", i);
				citAi.setValue(pmid);
				i++;

			}

			ce.executeUpdateQuery(corVi);
			ce.commitTransaction();

		} finally {

			getCe().closeDbConnection();

		}

	}

	public void addCorpusToArticle(long articleBmkegId, long corpusBmkegId)
			throws Exception {

		ArticleCitation a = findArticleByVpdmfId(articleBmkegId);
		if (a == null) {
			throw new Exception("No article with id: " + articleBmkegId
					+ " was found for updating.");
		}

		List<Corpus> corpora = a.getCorpora();
		if (corpora == null) {
			corpora = new ArrayList<Corpus>();
			a.setCorpora(corpora);
		}

		if (doesCorporaContainsCorpus(corpora, corpusBmkegId)) {
			// Corpus already contained in article's corpora.
			return;
		}

		Corpus c = findCorpusById(corpusBmkegId);
		if (c == null) {
			throw new Exception("No corpus with id: " + corpusBmkegId
					+ " was found to add to the article's corpora");
		}

		corpora.add(c);

		updateArticleCitation(a);
	}

	public void addCorpusToArticles(long corpusBmkegId, long[] articlesBmkegIds)
			throws Exception {
		for (int i = 0; i < articlesBmkegIds.length; i++) {
			addCorpusToArticle(articlesBmkegIds[i], corpusBmkegId);
		}
	}

	private boolean doesCorporaContainsCorpus(List<Corpus> corpora,
			long corpusBmkegId) {

		for (Corpus c : corpora) {
			if (c.getVpdmfId() == corpusBmkegId)
				return true;
		}
		return false;
	}



}
