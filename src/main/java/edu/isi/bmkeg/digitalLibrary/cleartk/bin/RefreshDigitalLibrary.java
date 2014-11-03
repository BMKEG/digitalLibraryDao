package edu.isi.bmkeg.digitalLibrary.cleartk.bin;

import java.io.File;

import org.apache.uima.collection.CollectionReader;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.cleartk.opennlp.tools.SentenceAnnotator;
import org.cleartk.token.tokenizer.TokenAnnotator;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.uimafit.factory.AggregateBuilder;
import org.uimafit.factory.AnalysisEngineFactory;
import org.uimafit.factory.CollectionReaderFactory;
import org.uimafit.factory.TypeSystemDescriptionFactory;
import org.uimafit.pipeline.SimplePipeline;

import edu.isi.bmkeg.digitalLibrary.bin.ImportMedlineDirectory.Options;
import edu.isi.bmkeg.digitalLibrary.cleartk.annotators.SentencePerLineFTDWriter;
import edu.isi.bmkeg.digitalLibrary.cleartk.cr.DigitalLibraryCollectionReader;

public class RefreshDigitalLibrary {

	public static class Options {

		@Option(name = "-l", usage = "Database login")
		public String login = "";

		@Option(name = "-p", usage = "Database password")
		public String password = "";

		@Option(name = "-db", usage = "Database name")
		public String dbName = "";

		@Option(name = "-wd", usage = "Working Directory")
		public File workingDirectory;

	}

	public static void main(String[] args) throws Exception {

		Options options = new Options();

		CmdLineParser parser = new CmdLineParser(options);

		try {

			parser.parseArgument(args);

			TypeSystemDescription typeSystem = TypeSystemDescriptionFactory
					.createTypeSystemDescription("uimaTypes.vpdmf-digitalLibrary");

			CollectionReader cr = CollectionReaderFactory
					.createCollectionReader(
							DigitalLibraryCollectionReader.class, typeSystem,
							DigitalLibraryCollectionReader.LOGIN,
							options.login,
							DigitalLibraryCollectionReader.PASSWORD,
							options.password,
							DigitalLibraryCollectionReader.DB_URL,
							options.dbName,
							DigitalLibraryCollectionReader.WORKING_DIRECTORY,
							options.workingDirectory);

			AggregateBuilder builder = new AggregateBuilder();

			builder.add(SentenceAnnotator.getDescription()); // Sentence
															// segmentation
			
			builder.add(TokenAnnotator.getDescription()); // Tokenization

			// It would be better to write into the preprocessed instances the
			// whole token
			// (skip stemming for now) and do Stemming while processing
			// instances if desired.
			// So the instance processors can have both features, tokens and
			// stems [MT].
			//
			// builder.add(DefaultSnowballStemmer.getDescription("English")); 
			//
			// Stemming

			builder.add(AnalysisEngineFactory.createPrimitiveDescription(
					SentencePerLineFTDWriter.class,
					SentencePerLineFTDWriter.PARAM_DIR_PATH, options.workingDirectory));

			// ///////////////////////////////////////////
			// Run pipeline to create training data file
			// ///////////////////////////////////////////
			SimplePipeline
					.runPipeline(cr, builder.createAggregateDescription());
			
		} catch (CmdLineException e) {

			System.err.println(e.getMessage());
			System.err.print("Arguments: ");
			parser.printSingleLineUsage(System.err);
			System.err.println("\n\n Options: \n");
			parser.printUsage(System.err);
			System.exit(-1);
		
		} catch (Exception e2) {
		
			e2.printStackTrace();
		
		}
			
	}

}
