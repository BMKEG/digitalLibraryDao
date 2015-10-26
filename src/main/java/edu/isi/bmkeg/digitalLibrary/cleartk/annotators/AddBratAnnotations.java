package edu.isi.bmkeg.digitalLibrary.cleartk.annotators;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
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
import bioc.type.MapEntry;
import bioc.type.UimaBioCAnnotation;
import bioc.type.UimaBioCDocument;
import bioc.type.UimaBioCLocation;
import bioc.type.UimaBioCPassage;
import edu.isi.bmkeg.digitalLibrary.utils.BioCUtils;
import edu.isi.bmkeg.utils.Converters;

public class AddBratAnnotations extends JCasAnnotator_ImplBase {

	public static final String BRAT_DATA_DIRECTORY = ConfigurationParameterFactory
			.createConfigurationParameterName(AddBratAnnotations.class,
					"bratDataDirectory");
	@ConfigurationParameter(mandatory = true, description = "Data directory to read annotation files from")
	protected String bratDataDirectory;

	public static final String BRAT_TYPE = ConfigurationParameterFactory
			.createConfigurationParameterName(AddBratAnnotations.class,
					"bratType");
	@ConfigurationParameter(mandatory = true, description = "Data directory to read annotation files from")
	protected String bratType;

	protected Map<String, File> dirLookup = new HashMap<String, File>();

	// Search for directories whose names are entirely numeric
	// (this is the convention we've adopted for brat annotations based on a
	// given pmid)
	private static Pattern dirPatt = Pattern.compile("^\\d+$");
	private static Pattern filePatt = Pattern.compile("\\.ann$");

	private StringMetric metric;
	private StringMetric metric2;
	
	private static Logger logger = Logger
			.getLogger(AddBratAnnotations.class);

	public void initialize(UimaContext context)
			throws ResourceInitializationException {

		super.initialize(context);

		try {

			File d = new File(bratDataDirectory);
			
			String[] fileTypes = {"ann"};
			Collection<File> l = (Collection<File>) FileUtils.listFiles(d, fileTypes, true);
			for( File f : l ) {
				File f2 = f.getParentFile();
				if( dirLookup.containsKey(f2.getName()))
					continue;
				Matcher m = dirPatt.matcher(f2.getName());
				if( f2.isDirectory() && m.find()) 
					dirLookup.put(f2.getName(), f2);
			}

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

		UimaBioCDocument uiD = JCasUtil.selectSingle(jCas, UimaBioCDocument.class);
		String pmid = uiD.getId();

		if(!dirLookup.containsKey(pmid))
			return;
		
		File d = dirLookup.get(pmid);
		String[] fileTypes = {"ann"};
		Collection<File> annFiles = (Collection<File>) FileUtils.listFiles(d, fileTypes, true);
		Map<String, File> fileLookup = new HashMap<String, File>();
		for( File f : annFiles ) {
			Matcher m = filePatt.matcher(f.getName());
			if( m.find()) 
				fileLookup.put(f.getName(), f);
		}

		FSArray passages = uiD.getPassages();

		Map<String, UimaBioCPassage> fragments = new HashMap<String, UimaBioCPassage>();
		for (int i=0; i<passages.size(); i++) {
			UimaBioCPassage frg = (UimaBioCPassage) passages.get(i);
			Map<String,String> infons = BioCUtils.convertInfons(frg.getInfons());
			fragments.put(infons.get("frgId"), frg);
		}

		for (File f : fileLookup.values()) {

			String fp = f.getAbsolutePath();
			String fileStem = fp.substring(0, fp.length() - 4);
			File txtFile = new File(fileStem + ".txt");
			
			String code = txtFile.getName().replaceAll("(" + uiD.getId()+"|_|\\.txt)", "");			
			if( !fragments.containsKey(code) ) {
				continue;
			}
			UimaBioCPassage frg = fragments.get(code);
			
			Scanner scanner;
			try {
				scanner = new Scanner(new FileInputStream(txtFile));
			} catch (FileNotFoundException e1) {
				// just skip this
				//throw new AnalysisEngineProcessException(e1);
				continue;
			}
			String text = scanner.useDelimiter("\\A").next();

			// Use the fragment offset of the annotation as the base
			// in the document and match the text to find the starting point.
			int[] psgStartEnd = {frg.getBegin(), frg.getEnd()};

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

			List<UimaBioCAnnotation> annotations = 
					new ArrayList<UimaBioCAnnotation>();
			
			for (Entity e : d_brat.getEntities()) {

				UimaBioCAnnotation a = new UimaBioCAnnotation(jCas);
				a.setId(e.getId());
				FSArray infons = new FSArray(jCas, 3);
				a.setInfons(infons);

				MapEntry mk1 = new MapEntry(jCas);
				mk1.setKey("type");
				mk1.setValue(this.bratType);
				a.getInfons().set(0, mk1);

				MapEntry mk2 = new MapEntry(jCas);
				mk2.setKey("value");
				mk2.setValue(e.getType());
				a.getInfons().set(1, mk2);

				MapEntry mk3 = new MapEntry(jCas);
				mk3.setKey("file");
				mk3.setValue(e.getFile());
				a.getInfons().set(2, mk3);

				a.setText(e.getString());
				
				FSArray locations = new FSArray(jCas, e.getLocations().size());
				a.setLocations(locations);
				for (int j = 0; j < e.getLocations().size(); j++) {
					Location l = e.getLocations().get(j);

					int[] startEnd = {psgStartEnd[0]+l.getStart(),
							psgStartEnd[0]+l.getEnd()};
					
					String check = docText.substring( startEnd[0], startEnd[1]);
					
					UimaBioCLocation biocL = new UimaBioCLocation(jCas);
					biocL.setBegin(startEnd[0]);
					biocL.setEnd(startEnd[1]);
					a.setBegin(startEnd[0]);
					a.setEnd(startEnd[1]);
					
					biocL.setOffset(startEnd[0]);
					biocL.setLength(startEnd[1] - startEnd[0]);
					a.getLocations().set(j, biocL);
					
					biocL.addToIndexes();
					a.addToIndexes();			

					logger.debug(e.getString() + "\n" + 
							"\t" + docText.substring(startEnd[0], startEnd[1]) );	

				}

				annotations.add(a);

			}
			
			FSArray oldAnnArray = frg.getAnnotations();
			FSArray newAnnArray = new FSArray(jCas, oldAnnArray.size() + annotations.size());
			for(int i=0; i<oldAnnArray.size(); i++) {
				newAnnArray.set(i, oldAnnArray.get(i));
			}			
			for(int i=0; i<annotations.size(); i++) {
				newAnnArray.set(oldAnnArray.size()+i, annotations.get(i));
			}
			oldAnnArray = null;
			frg.setAnnotations(newAnnArray);

		}
		
	}

}
