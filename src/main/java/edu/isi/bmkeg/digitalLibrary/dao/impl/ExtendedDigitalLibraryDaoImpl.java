package edu.isi.bmkeg.digitalLibrary.dao.impl;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import edu.isi.bmkeg.digitalLibrary.dao.ExtendedDigitalLibraryDao;
import edu.isi.bmkeg.digitalLibrary.model.citations.ArticleCitation;
import edu.isi.bmkeg.digitalLibrary.model.citations.Corpus;
import edu.isi.bmkeg.digitalLibrary.model.citations.Journal;
import edu.isi.bmkeg.digitalLibrary.model.citations.LiteratureCitation;
import edu.isi.bmkeg.digitalLibrary.model.qo.citations.ArticleCitation_qo;
import edu.isi.bmkeg.ftd.model.FTD;
import edu.isi.bmkeg.ftd.model.FTDFragmentBlock;
import edu.isi.bmkeg.ftd.model.qo.FTDFragment_qo;
import edu.isi.bmkeg.ftd.model.qo.FTD_qo;
import edu.isi.bmkeg.lapdf.model.LapdfDocument;
import edu.isi.bmkeg.lapdf.xml.model.LapdftextXMLDocument;
import edu.isi.bmkeg.uml.model.UMLclass;
import edu.isi.bmkeg.utils.Converters;
import edu.isi.bmkeg.utils.xml.XmlBindingTools;
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
public class ExtendedDigitalLibraryDaoImpl implements ExtendedDigitalLibraryDao {

	private static Logger logger = Logger.getLogger(ExtendedDigitalLibraryDaoImpl.class);

	// ~~~~~~~~~
	// Constants
	// ~~~~~~~~~
	private static final String CITATION_VIEW_NAME = "LiteratureCitation";
	private static final String ARTICLE_VIEW_NAME = "ArticleCitation";
	private static final String JOURNAL_VIEW_NAME = "Journal";
	private static final String CORPUS_VIEW_NAME = "Corpus";

	@Autowired
	private CoreDao coreDao;

	// ~~~~~~~~~~~~
	// Constructors
	// ~~~~~~~~~~~~

	public ExtendedDigitalLibraryDaoImpl() throws Exception {}

	public ExtendedDigitalLibraryDaoImpl(CoreDao coreDao) throws Exception {
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

			getCe().executeDeleteQuery(ARTICLE_VIEW_NAME, id );

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

			getCe().executeDeleteQuery(CORPUS_VIEW_NAME, corpus.getVpdmfId() );

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

	public ArticleCitation findArticleByPmid(int pmid) throws Exception {

		return (ArticleCitation) getCoreDao().findVBOGByAttributeValue(
				ARTICLE_VIEW_NAME, "LiteratureCitation", "ArticleCitation",
				"pmid", String.valueOf(pmid));
	}
	
	public ArticleCitation findArticleById(String idCode, Integer id) throws Exception {

		getCe().connectToDB();
		getCe().turnOffAutoCommit();

		ViewDefinition vd = getTop().getViews().get(ARTICLE_VIEW_NAME);
		ClassLoader cl = ExtendedDigitalLibraryDaoImpl.class.getClassLoader();
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

	public Corpus findCorpusByName(String name) throws Exception {

		return (Corpus) getCoreDao().findVBOGByAttributeValue(CORPUS_VIEW_NAME,
				"Corpus", "Corpus", "name", name);

	}
	
	public FTD findArticleDocumentByPmid(Integer pmid) throws Exception {

		getCe().connectToDB();
		getCe().turnOffAutoCommit();

		ViewDefinition vd = getTop().getViews().get("ArticleDocument");
		ClassLoader cl = ExtendedDigitalLibraryDaoImpl.class.getClassLoader();
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
		ClassLoader cl = ExtendedDigitalLibraryDaoImpl.class.getClassLoader();
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
		
		LapdftextXMLDocument xml = doc.convertToLapdftextXmlFormat();
		StringWriter writer = new StringWriter();
		XmlBindingTools.generateXML(xml, writer);
		ftd.setXml( writer.toString() );

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

	@Override
	/**
	 * Low level optimized SQL command to add articles to a given corpus.
	 */
	public int addArticlesToCorpusWithIds(List<Long> articleIds, long corpusId)
			throws Exception {
		
		int count = 0;
		
		ChangeEngine ce = (ChangeEngine) this.coreDao.getCe();
		VPDMf top = ce.readTop();
		ViewDefinition vd = top.getViews().get("ArticleCorpus");

		ViewInstance vi = new ViewInstance(vd);

		PrimitiveLink pl = (PrimitiveLink) vd.getSubGraph().getEdges()
				.iterator().next();
		UMLclass link = pl.getRole().getAss().getLinkClass();

		// We are going to write the data that we want to insert into the
		// database into
		// a local file and then insert that as a batch function.
		// - need to lock tables as we do this.
		ce.connectToDB();
		ce.turnOffAutoCommit();

		try {

			Collections.sort(articleIds);
			Iterator<Long> articleIt = articleIds.iterator();
			while (articleIt.hasNext()) {
				Long articleId = articleIt.next();
				
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
					count++;
				} 

			}

			ce.commitTransaction();

		} catch (Exception e) {
			
			throw e;

		} finally {

			this.coreDao.getCe().closeDbConnection();

		}
		
		return count;
		
	}
	
	public int removeArticlesFromCorpusWithIds(List<Long> articleIds, long corpusId) 
			throws Exception {
				
		int count = 0;
				
		ChangeEngine ce = (ChangeEngine) this.coreDao.getCe();
		VPDMf top = ce.readTop();

		ce.connectToDB();
		ce.turnOffAutoCommit();

		try {

			for(Long l : articleIds) {
						//
				// REMOVE EXISTING DATA FROM THE SET BACKING TABLE FOR THE 
				// SET BACKING TABLE DIRECTLY USING SQL
				//
				String sql = "DELETE c.* " +
							 "FROM Corpus_corpora__resources_LiteratureCitation AS c " +
							 "WHERE c.corpora_id = " + corpusId +
							 "  AND c.resources_id = " + l + ";";
				
				count += this.getCoreDao().getCe().executeRawUpdateQuery(sql);
				
				this.coreDao.getCe().prettyPrintSQL(sql);
			
			}

			ce.commitTransaction();

		} catch (Exception e) {
					
			throw e;

		} finally {

			this.coreDao.getCe().closeDbConnection();

		}
				
		return count;
				
	}
	
	public boolean fullyDeleteArticle(Long articleId) throws Exception {
				
		ChangeEngine ce = (ChangeEngine) this.coreDao.getCe();

		ce.connectToDB();
		ce.turnOffAutoCommit();

		try {
			
			//
			// preparation: find the ArticleDocument view for this citation. 
			//
			ArticleCitation_qo acQo = new ArticleCitation_qo();
			acQo.setVpdmfId(articleId + "");
			FTD_qo ftdQo = new FTD_qo();
			ftdQo.setCitation(acQo);
			List<LightViewInstance> lviList = this.coreDao.listInTrans(
					ftdQo, "ArticleDocument"
					);
						
			if( lviList.size() > 1 ) {
				throw new Exception("Too many documents returned from id:" + articleId);
			}

			// 3. remove the citation
			this.coreDao.deleteByIdInTrans(articleId, "ArticleCitation");					
			
			//
			// If there is an FTD available then we delete stufff...
			//
			if( lviList.size() == 1 ) {

				LightViewInstance ftd = lviList.get(0);
				
				FTDFragment_qo frgQo = new FTDFragment_qo();
				ftdQo = new FTD_qo();
				ftdQo.setVpdmfId(ftd.getVpdmfId() + "");
				frgQo.setFtd(ftdQo);
				
				Iterator<LightViewInstance> frgIt = this.coreDao.listInTrans(
						frgQo, "FTDFragment"
						).iterator();
				while( frgIt.hasNext() ) {
					LightViewInstance frg = frgIt.next();
					this.coreDao.deleteByIdInTrans(frg.getVpdmfId(), "FTDFragment");					
				}				
				
				// 2. remove the FTD
				this.coreDao.deleteByIdInTrans(ftd.getVpdmfId(), "FTD");					
				
			} 
			
			ce.commitTransaction();

		} catch (Exception e) {
					
			ce.rollbackTransaction();
			return false;			

		} finally {

			this.coreDao.getCe().closeDbConnection();

		}
				
		return true;
				
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

	public void addCorpusToArticle(long articleVpdmfId, long corpusVpdmfId)
			throws Exception {

		ArticleCitation a = this.coreDao.findById(articleVpdmfId, new ArticleCitation(), "ArticleCitation");
		if (a == null) {
			throw new Exception("No article with id: " + articleVpdmfId
					+ " was found for updating.");
		}

		List<Corpus> corpora = a.getCorpora();
		if (corpora == null) {
			corpora = new ArrayList<Corpus>();
			a.setCorpora(corpora);
		}

		if (doesCorporaContainsCorpus(corpora, corpusVpdmfId)) {
			// Corpus already contained in article's corpora.
			return;
		}

		Corpus c = this.coreDao.findById(corpusVpdmfId, new Corpus(), "Corpus");
		if (c == null) {
			throw new Exception("No corpus with id: " + corpusVpdmfId
					+ " was found to add to the article's corpora");
		}

		corpora.add(c);

		this.coreDao.update(a, "ArticleCitation");
	
	}
	
	private boolean doesCorporaContainsCorpus(List<Corpus> corpora,
			long corpusBmkegId) {

		for (Corpus c : corpora) {
			if (c.getVpdmfId() == corpusBmkegId)
				return true;
		}
		return false;
	
	}

	public void addCorpusToArticles(long corpusBmkegId, long[] articlesBmkegIds)
			throws Exception {
		for (int i = 0; i < articlesBmkegIds.length; i++) {
			addCorpusToArticle(articlesBmkegIds[i], corpusBmkegId);
		}
	}

	@Override
	public boolean removeFragmentBlock(FTDFragmentBlock frgBlk) throws Exception {

		int count = 0;
		long t = System.currentTimeMillis();
				
		ChangeEngine ce = (ChangeEngine) this.coreDao.getCe();
		
		VPDMf top = ce.readTop();

		try {

			ce.connectToDB();
			ce.turnOffAutoCommit();
	
			ViewDefinition vd = top.getViews().get("FTDFragment");
			ViewInstance qvi = new ViewInstance(vd);

			AttributeInstance ai = qvi.readAttributeInstance(
					"]FTDFragmentBlock|FTDFragmentBlock.x1", 0);
			ai.setValue(frgBlk.getX1());
			
			ai = qvi.readAttributeInstance(
					"]FTDFragmentBlock|FTDFragmentBlock.y1", 0);
			ai.setValue(frgBlk.getY1());
			
			ai = qvi.readAttributeInstance(
					"]FTDFragmentBlock|FTDFragmentBlock.x2", 0);
			ai.setValue(frgBlk.getX2());
			
			ai = qvi.readAttributeInstance(
					"]FTDFragmentBlock|FTDFragmentBlock.y2", 0);
			ai.setValue(frgBlk.getY2());
			
			ai = qvi.readAttributeInstance(
					"]FTDFragmentBlock|FTDFragmentBlock.x3", 0);
			ai.setValue(frgBlk.getX3());
			
			ai = qvi.readAttributeInstance(
					"]FTDFragmentBlock|FTDFragmentBlock.y3", 0);
			ai.setValue(frgBlk.getY3());
			
			ai = qvi.readAttributeInstance(
					"]FTDFragmentBlock|FTDFragmentBlock.x4", 0);
			ai.setValue(frgBlk.getX4());
			
			ai = qvi.readAttributeInstance(
					"]FTDFragmentBlock|FTDFragmentBlock.y4", 0);
			ai.setValue(frgBlk.getY4());
			
			ai = qvi.readAttributeInstance(
					"]FTDFragmentBlock|FTDFragmentBlock.p", 0);
			ai.setValue(frgBlk.getP());
			
			List<LightViewInstance> lLvi = ce.executeListQuery(qvi);
			
			if( lLvi.size() > 1 || lLvi.size() == 0 ) {
				return false;
			} 
		
			String sql = "DELETE frgBlk.* " +
					 "FROM FTDFragmentBlock AS frgBlk " +
					 "WHERE" +
					 " frgBlk.x1 = " + frgBlk.getX1() + " AND " +
					 " frgBlk.y1 = " + frgBlk.getY1() + " AND " +	
					 " frgBlk.x2 = " + frgBlk.getX2() + " AND " +	
					 " frgBlk.y2 = " + frgBlk.getY2() + " AND " +	
					 " frgBlk.x3 = " + frgBlk.getX3() + " AND " +	
					 " frgBlk.y3 = " + frgBlk.getY3() + " AND " +	
					 " frgBlk.x4 = " + frgBlk.getX4() + " AND " +	
					 " frgBlk.y4 = " + frgBlk.getY4() + " AND " +	
					 " frgBlk.p = " + frgBlk.getP() + ";";	
			
			int out = ce.executeRawUpdateQuery(sql);
			
			LightViewInstance lvi = lLvi.get(0);
			String[] idxTup = lvi.getIndexTuple().split("<|>");
			String[] idxTupFields = lvi.getIndexTupleFields().split("<|>");
			int countBlocks = -1;
			for(int i=0; i<idxTupFields.length; i++) {
				if( idxTupFields[i].equals( "FTDFragment_4" ) ){
					String[] xvalues = idxTup[i].split(",");
					countBlocks = xvalues.length;
					break;
				}
			}
			
			if( countBlocks == 1 ) {

				sql = "DELETE frg.* " +
						 "FROM FTDFragment AS frg " + 
						 "WHERE frg.vpdmfId = " + lvi.getVpdmfId() + ";";

				int out2 = ce.executeRawUpdateQuery(sql);

				sql = "DELETE vt.* " +
						 "FROM ViewTable AS vt " +
						 "WHERE vt.vpdmfId = " + lvi.getVpdmfId() + ";";

				int out3 = ce.executeRawUpdateQuery(sql);

			}
			
			ce.commitTransaction();
		
		} catch (Exception e) {
		
			e.printStackTrace();
			ce.rollbackTransaction();
			
		} finally {
			
			ce.dbConnection.close();
			
		}
		
		return true;
		
	
	}
	
}
