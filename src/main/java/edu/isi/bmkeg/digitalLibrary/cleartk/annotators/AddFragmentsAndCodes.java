
package edu.isi.bmkeg.digitalLibrary.cleartk.annotators;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.cas.FSArray;
import org.apache.uima.jcas.tcas.DocumentAnnotation;
import org.apache.uima.resource.ResourceInitializationException;
import org.cleartk.ml.feature.extractor.CleartkExtractor;
import org.cleartk.token.type.Sentence;
import org.cleartk.token.type.Token;
import org.simmetrics.StringMetric;
import org.simmetrics.StringMetricBuilder;
import org.simmetrics.metrics.CosineSimilarity;
import org.simmetrics.metrics.Levenshtein;
import org.simmetrics.simplifiers.CaseSimplifier;
import org.simmetrics.simplifiers.NonDiacriticSimplifier;
import org.simmetrics.tokenizers.QGramTokenizer;
import org.simmetrics.tokenizers.WhitespaceTokenizer;
import org.uimafit.component.JCasAnnotator_ImplBase;
import org.uimafit.descriptor.ConfigurationParameter;
import org.uimafit.factory.ConfigurationParameterFactory;
import org.uimafit.util.JCasUtil;

import bioc.type.UimaBioCAnnotation;
import bioc.type.UimaBioCDocument;
import bioc.type.UimaBioCLocation;
import bioc.type.UimaBioCPassage;
import bioc.type.UimaBioCSentence;
import edu.isi.bmkeg.digitalLibrary.controller.DigitalLibraryEngine;
import edu.isi.bmkeg.digitalLibrary.utils.BioCUtils;

public class AddFragmentsAndCodes extends JCasAnnotator_ImplBase {

	public static final String LOGIN = ConfigurationParameterFactory
			.createConfigurationParameterName(AddFragmentsAndCodes.class,
					"login");
	@ConfigurationParameter(mandatory = true, description = "Login for the Digital Library")
	protected String login;

	public static final String PASSWORD = ConfigurationParameterFactory
			.createConfigurationParameterName(AddFragmentsAndCodes.class,
					"password");
	@ConfigurationParameter(mandatory = true, description = "Password for the Digital Library")
	protected String password;

	public static final String WORKING_DIRECTORY = ConfigurationParameterFactory
			.createConfigurationParameterName(AddFragmentsAndCodes.class,
					"workingDirectory");
	@ConfigurationParameter(mandatory = true, description = "Working Directory for the Digital Library")
	protected String workingDirectory;
	
	public static final String DB_URL = ConfigurationParameterFactory
			.createConfigurationParameterName(AddFragmentsAndCodes.class,
					"dbUrl");
	@ConfigurationParameter(mandatory = true, description = "The Digital Library URL")
	protected String dbUrl;
	
	public static final String FRAGMENT_TYPE = ConfigurationParameterFactory
			.createConfigurationParameterName(AddFragmentsAndCodes.class,
					"fragmentType");
	@ConfigurationParameter(mandatory = true, description = "Fragment Type")
	protected String fragmentType;

	private DigitalLibraryEngine de;
	private String countSql;
	private String selectSql;
	private String fromWhereSql;
	private String orderBySql;
	
	private StringMetric cosineSimilarityMetric;
	private StringMetric levenshteinSimilarityMetric;
	
	private CleartkExtractor<DocumentAnnotation, Token> extractor;
	
	private static Logger logger = Logger
			.getLogger(AddFragmentsAndCodes.class);
	
	public void initialize(UimaContext context)
			throws ResourceInitializationException {

		super.initialize(context);
		
		try {
			
			de = new DigitalLibraryEngine();
			de.initializeVpdmfDao(
					login, 
					password, 
					dbUrl, 
					workingDirectory);
		} catch (Exception e) {

			throw new ResourceInitializationException(e);
		
		}	
		
		// Query based on a query constructed with SqlQueryBuilder based on the TriagedArticle view.
		countSql = "SELECT COUNT(*) ";

		selectSql = "SELECT l.vpdmfId, a.pmid, f.name, frg.vpdmfId, frg.frgOrder, blk.vpdmfOrder, blk.text, blk.code ";
		
		fromWhereSql = "FROM LiteratureCitation AS l," +
				" ArticleCitation as a, FTD as f, " + 
				" FTDFragment as frg, FTDFragmentBlock as blk " +
				" WHERE " +
				"blk.fragment_id = frg.vpdmfId AND " +
				"l.fullText_id = f.vpdmfId AND " +
				"l.vpdmfId = a.vpdmfId AND " +
				"frg.ftd_id = f.vpdmfId AND " +
				"frg.frgType = '"+ fragmentType + "' ";
		
		orderBySql = " ORDER BY l.vpdmfId, frg.vpdmfId, frg.frgOrder, blk.vpdmfOrder;";
		
		cosineSimilarityMetric = new StringMetricBuilder()
			.with(new CosineSimilarity<String>())
			.simplify(new CaseSimplifier.Lower())
			.simplify(new NonDiacriticSimplifier())
			.tokenize(new WhitespaceTokenizer())
			.tokenize(new QGramTokenizer(2))
			.build();

		levenshteinSimilarityMetric = new StringMetricBuilder()
			.with(new Levenshtein())
			.simplify(new NonDiacriticSimplifier())
			.build();
		
	}

	public void process(JCas jCas) throws AnalysisEngineProcessException {
		
		UimaBioCDocument uiD = JCasUtil.selectSingle(jCas, UimaBioCDocument.class);

		String pmidWhereSql = " AND a.pmid = '" + uiD.getId() + "' ";

		try {
			
			String txt = jCas.getDocumentText();
			List<Sentence> sentences = new ArrayList<Sentence>();
			for (Sentence sentence : JCasUtil.select(jCas, Sentence.class)) {
				sentences.add(sentence);
			}
			
			de.getDigLibDao().getCoreDao().getCe().connectToDB();
			
			ResultSet countRs = de.getDigLibDao().getCoreDao().getCe().executeRawSqlQuery(
					countSql + fromWhereSql + pmidWhereSql + orderBySql);
			
			countRs.next();
			int count = countRs.getInt(1);
			countRs.close();
			
			ResultSet rs = de.getDigLibDao().getCoreDao().getCe().executeRawSqlQuery(
					selectSql + fromWhereSql + pmidWhereSql + orderBySql);
			
			String frgText = "";
			int j = 0, pos = 0;
			List<UimaBioCAnnotation> frgBlockList = null;
			
			Map<String,String> frgTextHash = new HashMap<String,String>();
			Map<String,List<UimaBioCAnnotation>> frgBlockListHash =
					new HashMap<String,List<UimaBioCAnnotation>>();
			
			while( rs.next() ) {

				String frgOrder = rs.getString("frg.frgOrder");
				String blkId = rs.getString("blk.vpdmfOrder");
				
				String blkCode = rs.getString("blk.code");
				if( blkCode == null || blkCode.equals("-") ) 
					continue;
				
				String blkText = rs.getString("blk.text");
				blkText = blkText.replaceAll("\\s+", " ");
				blkText = blkText.replaceAll("\\-\\s+", "");
								
				// Parse the frgOrder code.
				// First split any '+' codes
				// then enumerate numbers for figures and ignore Supplemental data. 
				if(  !frgTextHash.containsKey(frgOrder) ) {
					frgText = "";
					frgBlockList = new ArrayList<UimaBioCAnnotation>();
				} else {
					frgText = frgTextHash.get(frgOrder);
					frgBlockList = frgBlockListHash.get(frgOrder);
				}
							
				frgText += blkText;
							
				int start = frgText.indexOf(blkText);
				int end = start + blkText.length() - 1;
							
				frgTextHash.put( frgOrder , frgText);

				UimaBioCAnnotation frgBlock = new UimaBioCAnnotation(jCas);
				frgBlock.setBegin(start);
				frgBlock.setEnd(end);
				Map<String,String> infons2 = new HashMap<String, String>();
				infons2.put("type", "epistSeg");
				infons2.put("value", blkCode);
				frgBlock.setInfons(BioCUtils.convertInfons(infons2, jCas));
				frgBlock.setText(blkText);
				frgBlockList.add(frgBlock);
				
				frgBlockListHash.put( frgOrder, frgBlockList);
				
				pos += pos + blkText.length();
								
			}
			
			rs.close();
			
			List<String> codes = new ArrayList<String>(frgTextHash.keySet());
			Collections.sort(codes);
			
			//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
			// SET UP THE BIOCDOCUMENT TO HOUSE THE FRAGMENTS AS BIOCPASSAGES
			// 
			Collection<UimaBioCDocument> uiDs =  JCasUtil.select(jCas,
					UimaBioCDocument.class);
			if( uiDs.size() != 1 ) {
				throw new Exception( "Number of BioCDocuments linked to " +
						"this document = " + uiDs.size() + ", this should be 1.");
			}
			FSArray passages = new FSArray(jCas, codes.size());
			uiD.setPassages(passages);
			int passageCount = 0;
			int nSkip = 0;
			//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
			
			FRAGMENT_LOOP: for( String code: codes ) {
									
				frgText = frgTextHash.get(code);
			
				float best = 0;
				int sCount = 0;
				int bestCount = 0;
				int winL = 50;
				int l = frgText.length();
				Sentence bestS = null;	
				for (Sentence sentence : sentences) {
					int s = sentence.getBegin();
					int e = (l>winL)?(s+winL):(s+l);
					if( e > txt.length() )
						e = txt.length();
					String sText = txt.substring(s, e);
					final float result = cosineSimilarityMetric.compare(frgText.substring(0,winL), sText); 
					if( result > best ) {
						best = result;
						bestS = sentence;
						bestCount = sCount;
					}
					sCount++;
				}
				
				//
				// If we can't find a good match, skip this whole fragment. 
				//
				if( best < 0.70 ) {
					int w = (l>winL)?(winL):(l);
					System.err.println("ERROR(" + uiD.getId() + "_" + code + 
							"), score:" + best + 
							",\n   guess: " + txt.substring(bestS.getBegin(), bestS.getBegin()+w) 
							+ "\n   frg: " + 
							frgText.substring(0, w) + "\n");
					
					continue;
				}
				
				
				int nextCount = 0;
				Sentence thisS = sentences.get(bestCount + nextCount);
				int delta1 = frgText.length() - (thisS.getEnd() - bestS.getBegin());
				Sentence nextS = sentences.get(bestCount + nextCount + 1);
				int delta2 = frgText.length() - (nextS.getEnd() - bestS.getBegin());
				while( Math.abs(delta1) > Math.abs(delta2) ) {
					nextCount++;
					thisS = sentences.get(bestCount + nextCount);
					delta1 = frgText.length() - (thisS.getEnd() - bestS.getBegin());					
					
					if( bestCount + nextCount + 1 >= sentences.size() ) {
						continue FRAGMENT_LOOP;	
					}
					
					nextS = sentences.get(bestCount + nextCount + 1);
					delta2 = frgText.length() - (nextS.getEnd() - bestS.getBegin());					
				
				}
				
				//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
				UimaBioCPassage uiP = new UimaBioCPassage(jCas);
				FSArray annotations = new FSArray(jCas, 
						frgBlockListHash.get(code).size());
				int annotationCount = 0;
				uiP.setAnnotations(annotations);
				uiP.setBegin(bestS.getBegin());
				uiP.setEnd(thisS.getEnd());
				uiP.setOffset(bestS.getBegin());
				
				Map<String,String> infons = new HashMap<String, String>();
				infons.put("type", "Fragment");
				infons.put("frgId", code);
				
				uiP.setInfons(BioCUtils.convertInfons(infons, jCas));
				uiP.addToIndexes();
				passages.set(passageCount, uiP);
				passageCount++;
				
				//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
				// Add the sentences that this is dealing with here.
				FSArray uiSentences = new FSArray(jCas, 
						sentences.size());
				uiP.setSentences(uiSentences);
				int sentenceCount = 0;
				for(Sentence s : sentences) {
					UimaBioCSentence uiS = new UimaBioCSentence(jCas);
					uiS.setBegin(s.getBegin());
					uiS.setEnd(s.getEnd());
					uiS.addToIndexes();
					uiSentences.set(sentenceCount, uiS);
					uiS.setOffset(s.getBegin());
					sentenceCount++;
				}
				
				//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
				// Now locate each of the blocks 
				// within the text of the fragment
				frgBlockList = frgBlockListHash.get(code);
				for( UimaBioCAnnotation blk : frgBlockList ) {
					int ss = blk.getBegin() + uiP.getBegin();
					int ee = blk.getEnd() + uiP.getBegin();
					String tt = blk.getText();
					String ttNoWhite = tt.replaceAll("\\s+", "");
					if(ttNoWhite.endsWith("-"))
						ttNoWhite = ttNoWhite.substring(0,ttNoWhite.length()-1);
					String lastCharacter = ttNoWhite.substring(ttNoWhite.length()-1, ttNoWhite.length());
					
					int whiteCount = tt.length() - ttNoWhite.length();
					
					float best_s_score = 0;
					float best_e_score = 0;
					int win1 = 20;
					int win2 = (ttNoWhite.length() < win1) ? ttNoWhite.length() : win1;  
					int best_s = 0;	
					int best_e = 0;	
					
					for(int i = 0; i<win1; i++) {
						
						int s = ss + i;
						String baseText = txt.substring(s, s+tt.length()).replaceAll("\\s+", "");
						String sText = "";
						try {
							sText = (baseText.length()<win2)?baseText:baseText.substring(0, win2);
						} catch (Exception e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
						float s_result = levenshteinSimilarityMetric.compare(ttNoWhite.substring(0,win2), sText); 
						
						// Only permit this position to be included as a candidate if 
						// (A) it leads to a better match
						// (B) it's the same as the starting non-whitespace character of the original text
						// (B) it's not itself whitespace in the original text
						if( s_result > best_s_score 
								&& !txt.substring(s,s+1).equals(" ") 
								&& ttNoWhite.startsWith(sText.substring(0,1))) {
							best_s_score = s_result;
							best_s = s;
						}
						if( i > 0 ){
							s = ss - i;
							baseText = txt.substring(s, s+tt.length()).replaceAll("\\s+", "");
							sText = (baseText.length()<win2)?baseText:baseText.substring(0, win2);
							s_result = levenshteinSimilarityMetric.compare(ttNoWhite.substring(0,win2), sText); 
							if( s_result > best_s_score 
									&& !txt.substring(s,s+1).equals(" ") 
									&& ttNoWhite.startsWith(sText.substring(0,1))) {
								best_s_score = s_result;
								best_s = s;
							}
						}
						
						int e = ee + i;
						baseText = txt.substring(e-tt.length(), e).replaceAll("\\s+", "");
						String eText = (baseText.length()<win2)?
								baseText
								:baseText.substring(baseText.length()-win2, baseText.length());
						float e_result = levenshteinSimilarityMetric.compare(
								ttNoWhite.substring(ttNoWhite.length()-win2,ttNoWhite.length()), 
								eText); 
						
						// Interesting bug: Since some blocks end with '-' at the end of the line,
						// we have to relax the condition for the last character to match perfectly.
						if( e_result > best_e_score 
								&& !txt.substring(e-1,e).equals(" ") 
								&& ttNoWhite.endsWith(lastCharacter) ) {
							best_e_score = e_result;
							best_e = e;
						}
						if( i > 0 ){
							e = ee - i;
							baseText = txt.substring(e-tt.length(), e).replaceAll("\\s+", "");
							eText = (baseText.length()<win2)?
									baseText
									:baseText.substring(baseText.length()-win2, baseText.length());
							e_result = levenshteinSimilarityMetric.compare(
									ttNoWhite.substring(ttNoWhite.length()-win2,ttNoWhite.length()), 
									eText); 
							if( e_result > best_e_score 
									&& !txt.substring(e-1,e).equals(" ") 
									&& ttNoWhite.endsWith(lastCharacter) ) {
								best_e_score = e_result;
								best_e = e;
							}
						}
							
						if( best_s_score == 1.0 && best_e_score == 1.0 )
							break;
					}
					
					// Debug Checks
					if( best_s_score != 1.0 || best_e_score != 1.0 ) {
						logger.debug("\n\t" + blk.getText()  
								+ "\n\t\t" + txt.substring(best_s, best_e) 
								+ "\n");	
					}
					
					blk.setBegin(best_s);
					blk.setEnd(best_e);
					blk.addToIndexes();
					
					annotations.set(annotationCount, blk);
					annotationCount++;
					
					FSArray locations = new FSArray(jCas, 1);
					blk.setLocations(locations);
					UimaBioCLocation uiL = new UimaBioCLocation(jCas);
					locations.set(0, uiL);
					uiL.setOffset(best_s);
					uiL.setLength(best_e - best_s);
					//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
					
				}
								
			}
			
			
			//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
			int pTotal= 0, pCount = 0;
			for(int i=0; i<codes.size(); i++) {
				if( passages.get(i) != null )
					pTotal++;
			}
			FSArray newPassages = new FSArray(jCas, pTotal);
			for(int i=0; i<codes.size(); i++) {
				if( passages.get(i) != null ) {
					newPassages.set(pCount, passages.get(i));
					pCount++;
				}
			}
			passages = null;
			uiD.setPassages(newPassages);
			//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
			
			de.getDigLibDao().getCoreDao().getCe().closeDbConnection();

		} catch (Exception e) {
		
			throw new AnalysisEngineProcessException(e);
		
		} 
		

	}

}
