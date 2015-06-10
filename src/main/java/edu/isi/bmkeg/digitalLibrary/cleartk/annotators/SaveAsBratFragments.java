package edu.isi.bmkeg.digitalLibrary.cleartk.annotators;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.uimafit.component.JCasAnnotator_ImplBase;
import org.uimafit.descriptor.ConfigurationParameter;
import org.uimafit.factory.ConfigurationParameterFactory;
import org.uimafit.util.JCasUtil;

import bioc.type.MapEntry;
import bioc.type.UimaBioCAnnotation;
import bioc.type.UimaBioCPassage;
import edu.isi.bmkeg.digitalLibrary.uimaTypes.citations.ArticleCitation;
import edu.isi.bmkeg.digitalLibrary.uimaTypes.citations.Corpus;
import edu.isi.bmkeg.digitalLibrary.utils.BioCUtils;
import edu.isi.bmkeg.ftd.uimaTypes.FTD;
import edu.isi.bmkeg.utils.Converters;

public class SaveAsBratFragments extends JCasAnnotator_ImplBase {

	public final static String PARAM_DIR_PATH = ConfigurationParameterFactory
			.createConfigurationParameterName(SaveAsBratFragments.class,
					"dirPath");
	@ConfigurationParameter(mandatory = true, description = "The place to put the document files to be classified.")
	String dirPath;

	public static final String FRAGMENT_TYPE = ConfigurationParameterFactory
			.createConfigurationParameterName(SaveAsBratFragments.class,
					"frgType");
	@ConfigurationParameter(mandatory = true, description = "Fragment Type")
	protected String frgType;

	public static final String ANNOTATION_TYPE = ConfigurationParameterFactory
			.createConfigurationParameterName(SaveAsBratFragments.class,
					"annotationType");
	@ConfigurationParameter(mandatory = true, description = "Annotation Type")
	protected String annotationType;
	
	private File baseData;

	public void initialize(UimaContext context)
			throws ResourceInitializationException {

		super.initialize(context);

		this.dirPath = (String) context.getConfigParameterValue(PARAM_DIR_PATH);
		this.baseData = new File(this.dirPath);

		// set up the file system.
		//
		// corpus
		// |
		// +-- categoryLabel_<train|test|eval>.txt
		//
		// each category contains one line per document with line breaks
		// stripped away
		//
		try {

			if (baseData.exists()) {
				Map<String, File> filesToZip = Converters
						.recursivelyListFiles(this.baseData);
				try {
					SimpleDateFormat df = new SimpleDateFormat("MM-dd-yy-HHmm");
					String suffix = df.format(new Date());
					File targetZip = new File(this.baseData.getParentFile()
							.getPath()
							+ "/"
							+ "_"
							+ this.baseData.getName()
							+ "_" + suffix + ".zip");
					Converters.zipIt(filesToZip, targetZip);
				} catch (Exception e) {
				}
				Converters.recursivelyDeleteFiles(this.baseData);
			}
			this.baseData.mkdirs();

		} catch (Exception e) {

			throw new ResourceInitializationException(e);

		}

	}

	public void process(JCas jCas) throws AnalysisEngineProcessException {

		FTD ftd = JCasUtil.selectSingle(jCas, FTD.class);
		Corpus c = JCasUtil.selectSingle(jCas, Corpus.class);
		ArticleCitation ac = JCasUtil.selectSingle(jCas, ArticleCitation.class);

		File outDir = new File(baseData.getPath() 
				+ "/" + c.getName() 
				+ "/" + ac.getPmid());
		
		try {

			for (UimaBioCPassage uiP : JCasUtil.select(jCas, UimaBioCPassage.class)) {

				Map<String, String> uiPInf = BioCUtils.convertInfons(uiP.getInfons());
				if( !uiPInf.containsKey("frgId") ) {
					continue;
				}
		
				if (!outDir.exists()) {
					outDir.mkdirs();
				}
				
				File outFile = new File(outDir.getPath() + "/" + ac.getPmid()
						+ "_" + uiPInf.get("frgId") + ".txt");
				PrintWriter out = new PrintWriter(new BufferedWriter(
						new FileWriter(outFile, true)));
				out.println(uiP.getCoveredText());
				out.close();

				File annFile = new File(outDir.getPath() + "/" + ac.getPmid()
						+ "_" + uiPInf.get("frgId") + ".ann");
				out = new PrintWriter(new BufferedWriter(new FileWriter(
						annFile, true)));
				
				int i = 0;
				for (UimaBioCAnnotation a : JCasUtil.selectCovered(UimaBioCAnnotation.class, uiP)) {
					
					Map<String, String> aInf = BioCUtils.convertInfons(a.getInfons());
					if( !aInf.containsKey("type") || !aInf.get("type").equals(this.annotationType) ) {
						continue;
					}
										
					MapEntry me = (MapEntry) a.getInfons(0);
					String s = "T" + i  + "\t" + aInf.containsKey("epistemicCode") + " " + 
							(a.getBegin() - uiP.getBegin()) + " " +
							(a.getEnd() - uiP.getBegin()) + "\t" + 
							a.getCoveredText();
					out.println(s);
					i++;
				}
				out.close();

			}

		} catch (IOException e) {

			throw new AnalysisEngineProcessException(e);

		}

	}

}
