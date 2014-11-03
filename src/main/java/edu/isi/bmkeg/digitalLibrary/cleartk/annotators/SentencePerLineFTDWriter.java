package edu.isi.bmkeg.digitalLibrary.cleartk.annotators;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.cleartk.token.type.Sentence;
import org.cleartk.token.type.Token;
import org.uimafit.component.JCasAnnotator_ImplBase;
import org.uimafit.descriptor.ConfigurationParameter;
import org.uimafit.factory.ConfigurationParameterFactory;
import org.uimafit.util.JCasUtil;

import edu.isi.bmkeg.ftd.uimaTypes.FTD;
import edu.isi.bmkeg.utils.Converters;

public class SentencePerLineFTDWriter extends JCasAnnotator_ImplBase {

	public final static String PARAM_DIR_PATH = ConfigurationParameterFactory
			.createConfigurationParameterName( SentencePerLineFTDWriter.class, "dirPath" );
	@ConfigurationParameter(mandatory = true, description = "The place to put the document files to be classified.")
	String dirPath;
	
	private File baseData;
	
	public void initialize(UimaContext context)
			throws ResourceInitializationException {

		super.initialize(context);
		
		this.dirPath = (String) context.getConfigParameterValue(PARAM_DIR_PATH);
		this.baseData = new File(this.dirPath);
				
	}

	public void process(JCas jCas) throws AnalysisEngineProcessException {
		
		FTD doc = JCasUtil.selectSingle(jCas, FTD.class);
		String stem = doc.getName();
		stem = stem.substring(0, stem.lastIndexOf("."));
		File outFile = new File(baseData.getPath() + "/" + stem + ".txt" ); 
		
		try {
			
			PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(outFile, true)));

			for (Sentence sentence : JCasUtil.select(jCas, Sentence.class)) {
				List<Token> tokens = JCasUtil.selectCovered(jCas, Token.class, sentence);	
				if (tokens.size() <= 0) { continue; }
				
				List<String> tokenStrings = JCasUtil.toText(tokens);						
				for (int i = 0; i < tokens.size(); i++) {
					out.print(tokenStrings.get(i) + " ");
				}

				out.print("\n");
				
			}

			out.close();
			
			int pauseHere = 0;
		
		} catch (IOException e) {
			
			throw new AnalysisEngineProcessException(e);
			 
		}

	}

}
