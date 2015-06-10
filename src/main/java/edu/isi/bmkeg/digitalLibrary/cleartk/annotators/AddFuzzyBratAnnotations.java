package edu.isi.bmkeg.digitalLibrary.cleartk.annotators;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Pattern;

import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.cas.FSArray;
import org.apache.uima.resource.ResourceInitializationException;
import org.cleartk.token.type.Sentence;
import org.simmetrics.StringMetric;
import org.simmetrics.StringMetricBuilder;
import org.simmetrics.metrics.CosineSimilarity;
import org.simmetrics.metrics.NeedlemanWunch;
import org.simmetrics.simplifiers.CaseSimplifier;
import org.simmetrics.simplifiers.NonDiacriticSimplifier;
import org.simmetrics.tokenizers.QGramTokenizer;
import org.simmetrics.tokenizers.WhitespaceTokenizer;
import org.uimafit.component.JCasAnnotator_ImplBase;
import org.uimafit.descriptor.ConfigurationParameter;
import org.uimafit.factory.ConfigurationParameterFactory;
import org.uimafit.util.JCasUtil;

import au.com.nicta.csp.brateval.Annotations;
import au.com.nicta.csp.brateval.Document;
import au.com.nicta.csp.brateval.Entity;
import au.com.nicta.csp.brateval.Location;
import bioc.type.UimaBioCAnnotation;
import bioc.type.UimaBioCDocument;
import bioc.type.UimaBioCLocation;
import bioc.type.UimaBioCPassage;
import bioc.type.MapEntry;
import edu.isi.bmkeg.digitalLibrary.uimaTypes.citations.ArticleCitation;
import edu.isi.bmkeg.digitalLibrary.uimaTypes.citations.Corpus;
import edu.isi.bmkeg.ftd.uimaTypes.FTD;
import edu.isi.bmkeg.ftd.uimaTypes.FTDFragment;
import edu.isi.bmkeg.utils.Converters;

public class AddFuzzyBratAnnotations extends JCasAnnotator_ImplBase {

	public static final String BRAT_DATA_DIRECTORY = ConfigurationParameterFactory
			.createConfigurationParameterName(AddFuzzyBratAnnotations.class,
					"bratDataDirectory");
	@ConfigurationParameter(mandatory = true, description = "Data directory to read annotation files from")
	protected String bratDataDirectory;

	protected Map<String, File> dirLookup = new HashMap<String, File>();

	// Search for directories whose names are entirely numeric
	// (this is the convention we've adopted for brat annotations based on a
	// given pmid)
	private static Pattern dirPatt = Pattern.compile("^\\d+$");
	private static Pattern filePatt = Pattern.compile("\\.ann$");

	private StringMetric metric;
	private StringMetric metric2;

	public void initialize(UimaContext context)
			throws ResourceInitializationException {

		super.initialize(context);

		try {

			File d = new File(bratDataDirectory);
			dirLookup = Converters.recursivelyListFiles(d, dirPatt);

			metric = new StringMetricBuilder()
					.with(new CosineSimilarity<String>())
					.simplify(new CaseSimplifier.Lower())
					.simplify(new NonDiacriticSimplifier())
					.tokenize(new WhitespaceTokenizer())
					.tokenize(new QGramTokenizer(2)).build();

			metric2 = new StringMetricBuilder()
			.with(new NeedlemanWunch())
			.simplify(new CaseSimplifier.Lower())
			.simplify(new NonDiacriticSimplifier()).build();
		} catch (Exception e) {

			throw new ResourceInitializationException(e);

		}

	}

	public void process(JCas jCas) throws AnalysisEngineProcessException {
		String docText = jCas.getDocumentText();
		List<Sentence> sentences = new ArrayList<Sentence>();
		for (Sentence sentence : JCasUtil.select(jCas, Sentence.class)) {
			sentences.add(sentence);
		}

		FTD ftd = JCasUtil.selectSingle(jCas, FTD.class);
		Corpus c = JCasUtil.selectSingle(jCas, Corpus.class);
		ArticleCitation ac = JCasUtil.selectSingle(jCas, ArticleCitation.class);
		long pmid = ac.getPmid();

		File d = dirLookup.get(bratDataDirectory + "/" + pmid);
		Map<String, File> fileLookup;
		try {
			fileLookup = Converters.recursivelyListFiles(d, filePatt);
		} catch (Exception e2) {
			return;
			//throw new AnalysisEngineProcessException(e2);
		}

		// Add document to collection
		UimaBioCDocument document = new UimaBioCDocument(jCas);
		FSArray passages = new FSArray(jCas, fileLookup.values().size());
		document.setPassages(passages);
		int docStart = 10000000;
		int docEnd = -1;

		Map<String, FTDFragment> fragments = new HashMap<String, FTDFragment>();
		for (FTDFragment frg : JCasUtil.select(jCas, FTDFragment.class)) {
			fragments.put(frg.getFrgOrder(), frg);
		}

		int p = 0;
		for (File f : fileLookup.values()) {

			String fp = f.getAbsolutePath();
			String fileStem = fp.substring(0, fp.length() - 4);
			File txtFile = new File(fileStem + ".txt");
			Scanner scanner;
			try {
				scanner = new Scanner(new FileInputStream(txtFile));
			} catch (FileNotFoundException e1) {
				// just skip this
				//throw new AnalysisEngineProcessException(e1);
				continue;
			}
			String text = scanner.useDelimiter("\\A").next();

			// Assume that the brat text is one or more sentences
			// in the document and match the text to find the starting point.
			int[] psgStartEnd = locateTextInLargerText(docText, text, sentences);
			
			if (psgStartEnd == null)
				continue;

			if( psgStartEnd[0] < docStart )
				docStart = psgStartEnd[0];

			if( psgStartEnd[1] > docEnd )
				docEnd = psgStartEnd[1];

			String[] temp = f.getName().substring(0, f.getName().length() - 4)
					.replaceAll(pmid + "_", "").split("_");
			String frgOrder = temp[0];

			Document d_brat = new Document();

			boolean ann = true;
			try {
				Annotations.read(f.getPath(), "ann", d_brat);
			} catch (Exception e) {
				ann = false;
			}

			// Set the id to the name of the file
			document.setId(f.getName().replaceAll(".txt$", ""));

			UimaBioCPassage passage = new UimaBioCPassage(jCas);

			FSArray annotations = new FSArray(jCas, d_brat.getEntities().size());
			passage.setAnnotations(annotations);

			passage.setOffset(0);
			passage.setText(text);

			int i = 0;
			for (Entity e : d_brat.getEntities()) {

				UimaBioCAnnotation a = new UimaBioCAnnotation(jCas);
				a.setId(e.getId());
				FSArray infons = new FSArray(jCas, 2);
				a.setInfons(infons);

				MapEntry mk1 = new MapEntry(jCas);
				mk1.setKey("type");
				mk1.setValue(e.getType());
				a.getInfons().set(0, mk1);

				MapEntry mk2 = new MapEntry(jCas);
				mk2.setKey("file");
				mk2.setValue(e.getFile());
				a.getInfons().set(1, mk2);

				a.setText(e.getString());
				
				FSArray locations = new FSArray(jCas, e.getLocations().size());
				a.setLocations(locations);
				for (int j = 0; j < e.getLocations().size(); j++) {
					Location l = e.getLocations().get(j);

					int[] startEnd = locateAnnotationInLargerText( 
							psgStartEnd[0] + l.getStart(), 
							psgStartEnd[0] + l.getEnd(), 
							docText, 
							psgStartEnd[0]+l.getStart(),
							psgStartEnd[0]+l.getEnd(),
							e.getString());
					if( startEnd[0]==0 || startEnd[1]==0 ) {
						System.err.println("ERROR: can't find " + e.getString() + " in " + 
								docText.substring(psgStartEnd[0]+l.getStart()-10,psgStartEnd[0]+l.getEnd()+10) +
								". Taking a best guess: " + docText.substring(psgStartEnd[0]+l.getStart(),psgStartEnd[0]+l.getEnd())
								);
						startEnd[0] = psgStartEnd[0]+l.getStart();
						startEnd[1] = psgStartEnd[0]+l.getEnd();
						
						//throw new AnalysisEngineProcessException(new Exception("can't find " + e.getString()));
					}
					String check = docText.substring( startEnd[0], startEnd[1]);

					
					UimaBioCLocation biocL = new UimaBioCLocation(jCas);
					biocL.setBegin(startEnd[0]);
					biocL.setEnd(startEnd[1]);
					a.setBegin(startEnd[0]);
					a.setEnd(startEnd[1]);
					
					biocL.setOffset(l.getStart());
					biocL.setLength(l.getEnd() - l.getStart());
					a.getLocations().set(j, biocL);
					
					biocL.addToIndexes();
					a.addToIndexes();			

				}

				passage.getAnnotations().set(i, a);
				i++;

			}

			document.getPassages().set(p, passage);
			p++;
			
		}
		
		document.setBegin(docStart);
		document.setEnd(docEnd);
		document.addToIndexes();			

	}

	private int[] locateTextInLargerText(String docText, String text,
			List<Sentence> sentences) {

		float best = 0;
		int sCount = 0;
		int bestCount = 0;
		int winL = 50;
		Sentence bestS = null;
		for (Sentence sentence : sentences) {
			int s = sentence.getBegin();
			int l = text.length();
			int e = (l > winL) ? (s + winL) : (s + l);
			if (e > docText.length())
				e = docText.length();
			String sText = docText.substring(s, e);
		
			final float result = metric.compare(
					text.substring(0, (l > winL) ? winL : l),
					sText);
			if (result > best) {
				best = result;
				bestS = sentence;
				bestCount = sCount;
			}
			sCount++;
		}

		//
		// If we can't find a good match, skip this whole fragment.
		//
		if (best < 0.70) {
			System.err.println("ERROR: score:" + best + " :"
					+ docText.substring(bestS.getBegin(), bestS.getEnd())
					+ "\n   " + docText + "\n");
			return null;
		}

		int nextCount = 0;
		Sentence thisS = sentences.get(bestCount + nextCount);
		int delta1 = text.length() - (thisS.getEnd() - bestS.getBegin());
		Sentence nextS = sentences.get(bestCount + nextCount + 1);
		int delta2 = text.length() - (nextS.getEnd() - bestS.getBegin());
		while (Math.abs(delta1) > Math.abs(delta2) && delta1 > 25) {
			nextCount++;
			thisS = sentences.get(bestCount + nextCount);
			delta1 = docText.length() - (thisS.getEnd() - bestS.getBegin());

			if (bestCount + nextCount + 1 >= sentences.size()) {
				return null;
			}

			nextS = sentences.get(bestCount + nextCount + 1);
			delta2 = docText.length() - (nextS.getEnd() - bestS.getBegin());

		}

		int[] beginEnd = { bestS.getBegin(), thisS.getEnd() };

		return beginEnd;

	}

	private int[] locateAnnotationInLargerText(int sWin, int eWin, String docText, 
			int guessStart, int guessEnd, String tt) {

		if(docText.substring(guessStart, guessEnd).equals(tt)){
			int[] beginEnd = { guessStart, guessEnd };
			return beginEnd;
		}		
		
		tt = tt.replaceAll("\\s+","");

		float best_s_score = 0;
		float best_e_score = 0;

		int win1 = 20;
		int win2 = (tt.length() < 5) ? tt.length() : 5;
		
		int best_s = 0;
		int best_e = 0;

		float best_score = 0;
		int last_best_s = 0;
		int last_best_e = 0;
		float last_best_score = 0;

		for (int i = 0; i < win1; i++) {

			int s = guessStart + i;
			String sText = docText.substring(s, s + win1).replaceAll("\\s+", "");
			sText = sText.substring(0,win2);
			
			float s_result = metric2.compare(tt.substring(0, win2), sText);
			if (s_result > best_s_score) { // use the first best score 
				best_s_score = s_result;
				best_s = s;
			}
			
			if( i>0 ){
				s = guessStart - i;
				sText = docText.substring(s, s + win1).replaceAll("\\s+", "");
				sText = sText.substring(0,win2);
				
				s_result = metric2.compare(tt.substring(0, win2), sText);
				if (s_result > best_s_score) { // use the first best score 
					best_s_score = s_result;
					best_s = s;
				}
			}
			
			int e = guessEnd + i;
			String eText = docText.substring(e - win1, e).replaceAll("\\s+", "");
			eText = eText.substring(eText.length()-win2,eText.length());
			
			String tText = tt.substring(tt.length() - win2, tt.length());
			
			float e_result = metric2.compare(tText, eText);
			if (e_result > best_e_score) { // use the first best score
				best_e_score = e_result;
				best_e = e;
			}
			
			if( i>0 ){
				e = guessEnd - i;
				eText = docText.substring(e - win1, e).replaceAll("\\s+", "");
				eText = eText.substring(eText.length()-win2,eText.length());
				
				tText = tt.substring(tt.length() - win2, tt.length());
				
				e_result = metric2.compare(tText, eText);
				if (e_result > best_e_score) { // use the first best score
					best_e_score = e_result;
					best_e = e;
				}
			}
			
		}
		
		if( docText.substring(best_s, best_s+1).matches("\\s") ) 
			best_s++;

		if( docText.substring(best_e, best_e+1).matches("\\s") ) 
			best_e--;

		int[] beginEnd = { best_s, best_e };

		return beginEnd;

	}

}
