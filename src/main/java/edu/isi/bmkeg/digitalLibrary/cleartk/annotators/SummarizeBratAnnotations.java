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

import bioc.type.UimaBioCAnnotation;
import bioc.type.MapEntry;
import edu.isi.bmkeg.digitalLibrary.uimaTypes.citations.ArticleCitation;
import edu.isi.bmkeg.digitalLibrary.uimaTypes.citations.Corpus;
import edu.isi.bmkeg.ftd.uimaTypes.FTD;
import edu.isi.bmkeg.ftd.uimaTypes.FTDFragment;
import edu.isi.bmkeg.utils.Converters;

public class SummarizeBratAnnotations extends JCasAnnotator_ImplBase {

	public final static String PARAM_FILE_PATH = ConfigurationParameterFactory
			.createConfigurationParameterName(SummarizeBratAnnotations.class,
					"outFilePath");
	@ConfigurationParameter(mandatory = true, description = "The place to put the document files to be classified.")
	String outFilePath;

	private File outFile;

	public void initialize(UimaContext context)
			throws ResourceInitializationException {

		super.initialize(context);

		this.outFilePath = (String) context.getConfigParameterValue(PARAM_FILE_PATH);
		this.outFile = new File(this.outFilePath);


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

			if(this.outFile.exists())
				this.outFile.delete();
			
			PrintWriter out = new PrintWriter(new BufferedWriter(
					new FileWriter(outFile, true)));
			String s = "Pmid\tFrg\tTag\tValue\n";
			out.println(s);
			out.close();

		} catch (Exception e) {

			throw new ResourceInitializationException(e);

		}

	}

	public void process(JCas jCas) throws AnalysisEngineProcessException {

		FTD ftd = JCasUtil.selectSingle(jCas, FTD.class);
		Corpus c = JCasUtil.selectSingle(jCas, Corpus.class);
		ArticleCitation ac = JCasUtil.selectSingle(jCas, ArticleCitation.class);

		try {
			
			PrintWriter out = new PrintWriter(new BufferedWriter(
					new FileWriter(outFile, true)));

			for (UimaBioCAnnotation a : JCasUtil.select(jCas, UimaBioCAnnotation.class)) {
	
					MapEntry me = (MapEntry) a.getInfons(0);
					String s = ac.getPmid() + "\t" + 
								me.getValue() + "\t" + a.getCoveredText();
					out.println(s);

			}

			out.close();

		} catch (IOException e) {

			throw new AnalysisEngineProcessException(e);

		}

	}

}
