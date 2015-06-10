package edu.isi.bmkeg.digitalLibrary.cleartk.annotators;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

import javax.xml.stream.XMLStreamException;

import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.uimafit.component.JCasAnnotator_ImplBase;
import org.uimafit.descriptor.ConfigurationParameter;
import org.uimafit.factory.ConfigurationParameterFactory;
import org.uimafit.util.JCasUtil;

import bioc.BioCCollection;
import bioc.BioCDocument;
import bioc.io.BioCCollectionWriter;
import bioc.io.BioCFactory;
import bioc.type.UimaBioCDocument;

import com.google.gson.Gson;

import edu.isi.bmkeg.digitalLibrary.utils.BioCUtils;

public class SaveAsBioC extends JCasAnnotator_ImplBase {

	public final static String PARAM_FILE_PATH = ConfigurationParameterFactory
			.createConfigurationParameterName(SaveAsBioC.class,
					"outFilePath");
	@ConfigurationParameter(mandatory = true, description = "The place to put the document files to be classified.")
	String outFilePath;

	public static String XML = ".xml";
	public static String JSON = ".json";
	public final static String PARAM_FORMAT = ConfigurationParameterFactory
			.createConfigurationParameterName(SaveAsBioC.class,
					"outFileFormat");
	@ConfigurationParameter(mandatory = true, description = "The format of the output.")
	String outFileFormat;

	
	private File outFile;
	private BioCCollection collection;

	public void initialize(UimaContext context)
			throws ResourceInitializationException {

		super.initialize(context);

		this.outFilePath = (String) context
				.getConfigParameterValue(PARAM_FILE_PATH);
		this.outFile = new File(this.outFilePath);

		this.collection = new BioCCollection();

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

			if (this.outFile.exists())
				this.outFile.delete();

		} catch (Exception e) {

			throw new ResourceInitializationException(e);

		}

	}

	public void process(JCas jCas) throws AnalysisEngineProcessException {

		for (UimaBioCDocument uiD : JCasUtil.select(jCas,
				UimaBioCDocument.class)) {
			
			BioCDocument d = BioCUtils.convertUimaBioCDocument(uiD);
		
			String dirStr = this.outFile.getParent();
			Map<String, String> infons = BioCUtils.convertInfons(uiD.getInfons());
			File textFile = new File(dirStr + "/" + infons.get("source") + ".json");
			PrintWriter out;
			try {
				out = new PrintWriter(new BufferedWriter(
						new FileWriter(textFile, true)));
			} catch (IOException e) {
				throw new AnalysisEngineProcessException(e);
			}
			out.write(uiD.getCoveredText());
			out.close();
			
		}

	}

	public void collectionProcessComplete()
			throws AnalysisEngineProcessException {
		super.collectionProcessComplete();

		try {

			if (outFile.getName().endsWith(".xml")) {

				BioCCollectionWriter writer = BioCFactory.newFactory(
						BioCFactory.STANDARD).createBioCCollectionWriter(
						new FileWriter(outFile));

				writer.writeCollection(this.collection);

				writer.close();

			} else if (outFile.getName().endsWith(".json")) {

				Gson gson = new Gson();
				String json = gson.toJson(this.collection);

				PrintWriter out = new PrintWriter(new BufferedWriter(
						new FileWriter(outFile, true)));

				out.write(json);

				out.close();
				
			} else {
				
				throw new AnalysisEngineProcessException(
						new Exception("Please write to an *.xml or a *.json file")
						);
			}

		} catch (IOException | XMLStreamException e) {

			throw new AnalysisEngineProcessException(e);

		}

	}

}
