package edu.isi.bmkeg.digitalLibrary.dao.impl;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
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

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import edu.isi.bmkeg.digitalLibrary.dao.ExtendedDigitalLibraryDao;
import edu.isi.bmkeg.digitalLibrary.model.citations.ArticleCitation;
import edu.isi.bmkeg.digitalLibrary.model.citations.Corpus;
import edu.isi.bmkeg.digitalLibrary.model.citations.JournalEpoch;
import edu.isi.bmkeg.digitalLibrary.model.qo.citations.ArticleCitation_qo;
import edu.isi.bmkeg.digitalLibrary.model.qo.citations.Corpus_qo;
import edu.isi.bmkeg.digitalLibrary.model.qo.citations.ID_qo;
import edu.isi.bmkeg.digitalLibrary.model.qo.citations.JournalEpoch_qo;
import edu.isi.bmkeg.digitalLibrary.model.qo.citations.Journal_qo;
import edu.isi.bmkeg.ftd.model.FTD;
import edu.isi.bmkeg.ftd.model.FTDFragmentBlock;
import edu.isi.bmkeg.ftd.model.FTDRuleSet;
import edu.isi.bmkeg.ftd.model.qo.FTDFragment_qo;
import edu.isi.bmkeg.ftd.model.qo.FTD_qo;
import edu.isi.bmkeg.lapdf.model.LapdfDocument;
import edu.isi.bmkeg.lapdf.xml.model.LapdftextXMLDocument;
import edu.isi.bmkeg.uml.model.UMLclass;
import edu.isi.bmkeg.utils.Converters;
import edu.isi.bmkeg.utils.xml.XmlBindingTools;
import edu.isi.bmkeg.vpdmf.controller.queryEngineTools.ChangeEngine;
import edu.isi.bmkeg.vpdmf.controller.queryEngineTools.ChangeEngineImpl;
import edu.isi.bmkeg.vpdmf.dao.CoreDao;
import edu.isi.bmkeg.vpdmf.model.definitions.PrimitiveDefinition;
import edu.isi.bmkeg.vpdmf.model.definitions.PrimitiveLink;
import edu.isi.bmkeg.vpdmf.model.definitions.VPDMf;
import edu.isi.bmkeg.vpdmf.model.definitions.ViewDefinition;
import edu.isi.bmkeg.vpdmf.model.instances.AttributeInstance;
import edu.isi.bmkeg.vpdmf.model.instances.ClassInstance;
import edu.isi.bmkeg.vpdmf.model.instances.LightViewInstance;
import edu.isi.bmkeg.vpdmf.model.instances.PrimitiveInstance;
import edu.isi.bmkeg.vpdmf.model.instances.ViewInstance;

@Repository
public class ExtendedDigitalLibraryDaoImpl implements ExtendedDigitalLibraryDao {

	private static Logger logger = Logger
			.getLogger(ExtendedDigitalLibraryDaoImpl.class);

	@Autowired
	private CoreDao coreDao;

	// ~~~~~~~~~~~~
	// Constructors
	// ~~~~~~~~~~~~

	public ExtendedDigitalLibraryDaoImpl() throws Exception {
	}

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

	private ChangeEngine getCe() {
		return coreDao.getCe();
	}

	private VPDMf getTop() {
		return coreDao.getTop();
	}

	// ~~~~~~~~~~~~~~~~~~~~~
	// Convenience Functions
	// ~~~~~~~~~~~~~~~~~~~~~

	@Override
	public ArticleCitation findArticleByPmidInTrans(Integer pmid)
			throws Exception {

		ArticleCitation_qo acQo = new ArticleCitation_qo();
		acQo.setPmid(pmid + "");
		List<LightViewInstance> listLvi = this.coreDao.listInTrans(acQo,
				"ArticleCitation");
		if (listLvi.size() != 1) {
			return null;
		}

		LightViewInstance lvi = listLvi.get(0);
		ArticleCitation a = this.coreDao.findByIdInTrans(lvi.getVpdmfId(),
				new ArticleCitation(), "ArticleCitation");

		return a;

	}

	@Override
	public ArticleCitation findArticleByIdInTrans(String idCode, Integer id)
			throws Exception {

		ArticleCitation_qo acQo = new ArticleCitation_qo();
		ID_qo idQo = new ID_qo();
		acQo.getIds().add(idQo);
		idQo.setIdType(idCode);
		idQo.setIdValue(id + "");

		List<LightViewInstance> listLvi = this.coreDao.listInTrans(acQo,
				"ArticleCitation");
		if (listLvi.size() != 1) {
			return null;
		}

		LightViewInstance lvi = listLvi.get(0);
		ArticleCitation a = this.coreDao.findByIdInTrans(lvi.getVpdmfId(),
				new ArticleCitation(), "ArticleCitation");

		return a;

	}

	@Override
	public Corpus findCorpusByNameInTrans(String name) throws Exception {

		Corpus_qo cQo = new Corpus_qo();
		cQo.setName(name);

		List<LightViewInstance> listLvi = this.coreDao.listInTrans(cQo,
				"Corpus");
		if (listLvi.size() != 1) {
			return null;
		}

		LightViewInstance lvi = listLvi.get(0);
		Corpus c = this.coreDao.findByIdInTrans(lvi.getVpdmfId(), new Corpus(),
				"Corpus");

		return c;

	}

	@Override
	public FTD findArticleDocumentByPmidInTrans(Integer pmid) throws Exception {

		FTD_qo dQo = new FTD_qo();
		ArticleCitation_qo acQo = new ArticleCitation_qo();
		dQo.setCitation(acQo);
		acQo.setPmid(pmid + "");

		List<LightViewInstance> listLvi = this.coreDao.listInTrans(dQo,
				"ArticleDocument");
		if (listLvi.size() != 1) {
			return null;
		}

		LightViewInstance lvi = listLvi.get(0);
		FTD d = this.coreDao.findByIdInTrans(lvi.getVpdmfId(), new FTD(),
				"ArticleDocument");

		return d;

	}

	@Override
	public FTD findArticleDocumentByIdInTrans(String idCode, Integer id)
			throws Exception {

		FTD_qo dQo = new FTD_qo();
		ArticleCitation_qo acQo = new ArticleCitation_qo();
		dQo.setCitation(acQo);
		ID_qo idQo = new ID_qo();
		acQo.getIds().add(idQo);
		idQo.setIdType(idCode);
		idQo.setIdValue(id + "");

		List<LightViewInstance> listLvi = this.coreDao.listInTrans(dQo,
				"ArticleDocument");
		if (listLvi.size() != 1) {
			return null;
		}

		LightViewInstance lvi = listLvi.get(0);
		FTD d = this.coreDao.findByIdInTrans(lvi.getVpdmfId(), new FTD(),
				"ArticleDocument");

		return d;

	}

	// ~~~~~~~~~~~~~~
	// List functions
	// ~~~~~~~~~~~~~~

	public Map<Integer, Long> lookupPmidsInTrans(Set<Integer> pmids)
			throws Exception {

		Map<Integer, Long> pmidMap = new HashMap<Integer, Long>();

		Iterator<Integer> it = pmids.iterator();
		while (it.hasNext()) {
			Integer pmid = it.next();

			ArticleCitation_qo acQo = new ArticleCitation_qo();
			acQo.setPmid(pmid.toString());
			ArticleCitation ac = new ArticleCitation();
			List<ArticleCitation> lci = this.getCoreDao().listClassInTrans(
					acQo, ac);

			if (lci.size() == 1) {
				pmidMap.put(pmid, lci.get(0).getVpdmfId());
			} else if (lci.size() == 0) {
				logger.debug("Can't find pmid: " + pmid);
			} else {
				logger.info("pmmid: " + pmid + " ambiguous ");
			}

		}

		return pmidMap;

	}

	// ~~~~~~~~~~~~~~~~~~~~
	// Add x to y functions
	// ~~~~~~~~~~~~~~~~~~~~
	@Override
	public long addPdfToArticleCitation(LapdfDocument doc, ArticleCitation ac,
			File pdf) throws Exception {

		FTD ftd = new FTD();
		String wd = this.getCoreDao().getWorkingDirectory();
		
		String dirPth = "pdfs/" + ac.getJournal().getAbbr() + 
				"/" + ac.getPubYear() + "/" + ac.getVolValue();
		dirPth = dirPth.replaceAll("\\s+", "_");
		File pdfDir = new File(wd + "/" + dirPth);	
		File newPdf = new File(wd + "/" + dirPth + "/" + pdf.getName());
		
		boolean status = pdfDir.mkdirs();
		
		// Copy file intot te 
		FileUtils.copyFile(pdf, newPdf);
		
		//
		// Here is where we run the pdf2Swf command.
		//
		String pth = dirPth + "/" + pdf.getName();
		addSwfToFtd(newPdf, ftd);

		ftd.setChecksum(Converters.checksum(newPdf));
		ftd.setName(pth);
		
		String pthStem = pth.substring(0,pth.length()-4);

		/*PmcXmlArticle pmcXml = doc.convertToPmcXmlFormat();
		StringWriter writer = new StringWriter();
		XmlBindingTools.generateXML(pmcXml, writer);
		String pmcXmlStr = writer.toString();
		File pmcXmlFile = new File( wd + "/" + pthStem + "_pmc.xml" );
		FileUtils.writeStringToFile(pmcXmlFile, pmcXmlStr);
		ftd.setPmcXmlFile(pthStem + "_pmc.xml");*/
		
		LapdftextXMLDocument xml = doc.convertToLapdftextXmlFormat();
		File xmlFile = new File( wd + "/" + pthStem + "_lapdf.xml" );
		FileWriter writer = new FileWriter(xmlFile);
		XmlBindingTools.generateXML(xml, writer);
		ftd.setXmlFile( pthStem + "_lapdf.xml");

		ftd.setCitation(ac);
		ac.setFullText(ftd);

		long adId = -1;

		//
		// Note that we do not set rule sets to link to ftd's here
		// We handle the assigment of rule sets via journal epochs.
		// ftd.setRuleSet(rs);
		//
		FTD_qo ftdQo = new FTD_qo();
		ftdQo.setChecksum(ftd.getChecksum());
		List<LightViewInstance> lviList = this.getCoreDao().listInTrans(ftdQo,
				"ArticleDocument");

		if (lviList.size() == 0) {
			adId = this.getCoreDao().insertInTrans(ftd, "ArticleDocument");
		} else if (lviList.size() == 1) {
			ftd.setVpdmfId(lviList.get(0).getVpdmfId());
			adId = this.getCoreDao().updateInTrans(ftd, "ArticleDocument");
		} else {
			throw new Exception("Ambiguous data");
		}

		return adId;

	}

	public String addSwfToFtd(File pdf, FTD ftd) throws Exception, IOException {

		File swfBinDir = Converters.readAppDirectory("swftools");
		String swfPath = swfBinDir + "/pdf2swf";
		if (System.getProperty("os.name").toLowerCase().contains("win")) {
			swfPath += ".exe";
		}

		String pdfStem = pdf.getName().replaceAll("\\.pdf", "");
		File swfFile = new File(pdf.getParent() + "/" + pdfStem + ".swf");

		Process p = Runtime.getRuntime().exec(swfPath + " " + 
						pdf.getPath() + " -o " + swfFile.getPath());

		if( p == null) {
			throw new Exception("Can't find pdf2swf application on the PATH");
		}
	
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

		if (!swfFile.exists()) {
			throw new Exception("pdf2swf-based swf generation failed: " + out);
		}

		String pth = swfFile.getPath();
		pth = pth.substring(this.getCoreDao().getWorkingDirectory().length(), 
				pth.length());
		ftd.setLaswfFile(pth);
		
		return pth;

	}

	@Override
	/**
	 * Low level optimized SQL command to add articles to a given corpus.
	 */
	public int addArticlesToCorpusWithIds(List<Long> articleIds, long corpusId)
			throws Exception {

		int count = 0;

		ChangeEngineImpl ce = (ChangeEngineImpl) this.coreDao.getCe();
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
				if (l.size() == 0) {
					ce.insertObjectIntoDB(linkCi);
					count++;
				}

			}

			ce.clearQuery();
			ce.commitTransaction();

		} catch (Exception e) {

			throw e;

		} finally {

			this.coreDao.getCe().closeDbConnection();

		}

		return count;

	}

	public int removeArticlesFromCorpusWithIds(List<Long> articleIds,
			long corpusId) throws Exception {

		int count = 0;

		ChangeEngineImpl ce = (ChangeEngineImpl) this.coreDao.getCe();
		VPDMf top = ce.readTop();

		ce.connectToDB();
		ce.turnOffAutoCommit();

		try {

			for (Long l : articleIds) {

				//
				// REMOVE EXISTING DATA FROM THE SET BACKING TABLE FOR THE
				// SET BACKING TABLE DIRECTLY USING SQL
				//
				String sql = "DELETE c.* "
						+ "FROM Corpus_corpora__resources_LiteratureCitation AS c "
						+ "WHERE c.corpora_id = " + corpusId
						+ "  AND c.resources_id = " + l + ";";

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

		ChangeEngineImpl ce = (ChangeEngineImpl) this.coreDao.getCe();

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
			List<LightViewInstance> lviList = this.coreDao.listInTrans(ftdQo,
					"ArticleDocument");

			if (lviList.size() > 1) {
				throw new Exception("Too many documents returned from id:"
						+ articleId);
			}

			// 3. remove the citation
			this.coreDao.deleteByIdInTrans(articleId, "ArticleCitation");

			//
			// If there is an FTD available then we delete stufff...
			//
			if (lviList.size() == 1) {

				LightViewInstance ftd = lviList.get(0);

				FTDFragment_qo frgQo = new FTDFragment_qo();
				ftdQo = new FTD_qo();
				ftdQo.setVpdmfId(ftd.getVpdmfId() + "");
				frgQo.setFtd(ftdQo);

				Iterator<LightViewInstance> frgIt = this.coreDao.listInTrans(
						frgQo, "FTDFragment").iterator();
				while (frgIt.hasNext()) {
					LightViewInstance frg = frgIt.next();
					this.coreDao.deleteByIdInTrans(frg.getVpdmfId(),
							"FTDFragment");
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

		// We are going to write the data that we want to insert into the
		// database into
		// a local file and then insert that as a batch function.
		// - need to lock tables as we do this.
		ChangeEngineImpl ce = (ChangeEngineImpl) this.coreDao.getCe();

		ce.connectToDB();
		ce.turnOffAutoCommit();

		Corpus c = this.findCorpusByNameInTrans(corpusName);
		if (c == null) {
			throw new Exception("Could not find a corpus named: " + corpusName);
		}
		Long corpusId = c.getVpdmfId();

		VPDMf top = ce.readTop();
		ViewDefinition vd = top.getViews().get("ArticleCorpus");

		PrimitiveLink pl = (PrimitiveLink) vd.getSubGraph().getEdges()
				.iterator().next();
		UMLclass link = pl.getRole().getAss().getLinkClass();

		Map<Integer, Long> bmkegIdMap = lookupPmidsInTrans(pmids);

		try {
			List<Integer> pmidList = new ArrayList<Integer>(pmids);
			Collections.sort(pmidList);
			Iterator<Integer> pmidIt = pmidList.iterator();
			while (pmidIt.hasNext()) {
				Integer pmid = pmidIt.next();
				Long articleId = bmkegIdMap.get(pmid);

				if (articleId == null)
					continue;

				ClassInstance linkCi = new ClassInstance(link);

				AttributeInstance corpusIdAi = linkCi.getAttributes().get(
						"corpora_id");
				corpusIdAi.setValue(corpusId);

				AttributeInstance articleIdAi = linkCi.getAttributes().get(
						"resources_id");
				articleIdAi.setValue(articleId);

				List<ClassInstance> l = ce.queryClass(linkCi);
				if (l.size() == 0) {
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
			ChangeEngineImpl ce = (ChangeEngineImpl) this.coreDao.getCe();
			VPDMf top = ce.readTop();

			ViewDefinition corVd = top.getViews().get("ArticleCorpus");
			ViewInstance queryVi = new ViewInstance(corVd);

			ce.connectToDB();
			ce.turnOffAutoCommit();

			ViewInstance qVi = new ViewInstance(corVd);
			PrimitiveDefinition articlePd = (PrimitiveDefinition) corVd
					.getSubGraph().getNodes().get("ArticleCitationLU");
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

				if (i >= corVi.countPrimitives(articlePd))
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

		ArticleCitation a = this.coreDao.findById(articleVpdmfId,
				new ArticleCitation(), "ArticleCitation");
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
	public boolean removeFragmentBlock(FTDFragmentBlock frgBlk)
			throws Exception {

		int count = 0;
		long t = System.currentTimeMillis();

		ChangeEngineImpl ce = (ChangeEngineImpl) this.coreDao.getCe();

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

			if (lLvi.size() > 1 || lLvi.size() == 0) {
				return false;
			}

			String sql = "DELETE frgBlk.* "
					+ "FROM FTDFragmentBlock AS frgBlk " + "WHERE"
					+ " frgBlk.x1 = "
					+ frgBlk.getX1()
					+ " AND "
					+ " frgBlk.y1 = "
					+ frgBlk.getY1()
					+ " AND "
					+ " frgBlk.x2 = "
					+ frgBlk.getX2()
					+ " AND "
					+ " frgBlk.y2 = "
					+ frgBlk.getY2()
					+ " AND "
					+ " frgBlk.x3 = "
					+ frgBlk.getX3()
					+ " AND "
					+ " frgBlk.y3 = "
					+ frgBlk.getY3()
					+ " AND "
					+ " frgBlk.x4 = "
					+ frgBlk.getX4()
					+ " AND "
					+ " frgBlk.y4 = "
					+ frgBlk.getY4()
					+ " AND "
					+ " frgBlk.p = "
					+ frgBlk.getP()
					+ ";";

			int out = ce.executeRawUpdateQuery(sql);

			LightViewInstance lvi = lLvi.get(0);
			String[] idxTup = lvi.getIndexTuple().split("\\{\\|\\}");
			String[] idxTupFields = lvi.getIndexTupleFields()
					.split("\\{\\|\\}");
			int countBlocks = -1;
			for (int i = 0; i < idxTupFields.length; i++) {
				if (idxTupFields[i].equals("FTDFragment_4")) {
					String[] xvalues = idxTup[i].split(",");
					countBlocks = xvalues.length;
					break;
				}
			}

			if (countBlocks == 1) {

				sql = "DELETE frg.* " + "FROM FTDFragment AS frg "
						+ "WHERE frg.vpdmfId = " + lvi.getVpdmfId() + ";";

				int out2 = ce.executeRawUpdateQuery(sql);

				sql = "DELETE vt.* " + "FROM ViewTable AS vt "
						+ "WHERE vt.vpdmfId = " + lvi.getVpdmfId() + ";";

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

	@Override
	public JournalEpoch retriveJournalEpochForCitation(ArticleCitation ac)
			throws Exception {

		JournalEpoch_qo jeQo = new JournalEpoch_qo();
		Journal_qo jQo = new Journal_qo();
		jeQo.setJournal(jQo);
		jQo.setAbbr(ac.getJournal().getAbbr());
		jeQo.setStartVol("<vpdmf-lteq>" + ac.getVolValue());
		jeQo.setEndVol("<vpdmf-gteq>" + ac.getVolValue());

		List<LightViewInstance> lviList = this.coreDao.listInTrans(jeQo,
				"JournalEpoch");

		if (lviList.size() == 1) {

			LightViewInstance lvi = lviList.get(0);
			return this.coreDao.findByIdInTrans(lvi.getVpdmfId(),
					new JournalEpoch(), "JournalEpoch");

		} else if (lviList.size() == 0) {

			return null;

		} else {

			throw new Exception("Ambiguous Journal Epoch for "
					+ ac.getJournal().getAbbr() + ", vol: " + ac.getVolValue());

		}

	}

	@Override
	public FTDRuleSet readRuleFileFromDisk(File ruleFile) throws Exception {

		String wdPth = this.getCoreDao().getWorkingDirectory();
		File ruleDir = new File(wdPth + "/rules");
		if( !ruleDir.exists() ) {
			ruleDir.mkdirs();
		}
			
		FTDRuleSet rs = new FTDRuleSet();
		String s = ruleFile.getName();
		s = s.substring(0, s.lastIndexOf("."));
		s = s.replaceAll("_drl", "");
		rs.setRsName(s);
		rs.setRsDescription("-");
		rs.setFileName(ruleFile.getName());
		rs.setFilePath("rules/" + ruleFile.getName());
		FileUtils.copyFileToDirectory(ruleFile, ruleDir);

		if (ruleFile.getName().endsWith(".drl")) {
			rs.setFileType("drl");
		} else if (ruleFile.getName().endsWith(".csv")) {
			rs.setFileType("csv");
		} else if (ruleFile.getName().endsWith(".xls")) {
			rs.setFileType("xls");
		}

		return rs;

	}

}
