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

import edu.isi.bmkeg.digitalLibrary.cleartk.annotators.AddBratAnnotations;
import edu.isi.bmkeg.digitalLibrary.cleartk.annotators.AddFragmentsAndCodes;
import edu.isi.bmkeg.digitalLibrary.cleartk.annotators.SummarizeBratAnnotations;
import edu.isi.bmkeg.digitalLibrary.cleartk.cr.DigitalLibraryCollectionReader;

public class CategorizeAnnotations {

	public static class Options {

		@Option(name = "-corpus", usage = "The target corpus to be evaluated", required = true)
		public String corpus = "";

		@Option(name = "-frgType", usage = "The fragment type to be used", required = true)
		public String frgType = "";

		@Option(name = "-frgCode", usage = "The fragment codes to be extracted", required = false)
		public String frgCode = "";

		@Option(name = "-l", usage = "Database login", required = true)
		public String login = "";

		@Option(name = "-p", usage = "Database password", required = true)
		public String password = "";

		@Option(name = "-db", usage = "Database name", required = true)
		public String dbName = "";

		@Option(name = "-wd", usage = "Working Directory", required = true)
		public File workingDirectory;

		@Option(name = "-inBrat", usage = "Brat Annotations", required = true)
		public File inBrat;

		@Option(name = "-out", usage = "Where to put the summaries", required = true)
		public File out;

	}

	public static void main(String[] args) throws Exception {

		Options options = new Options();

		CmdLineParser parser = new CmdLineParser(options);

		try {

			parser.parseArgument(args);

			TypeSystemDescription typeSystem = TypeSystemDescriptionFactory
					.createTypeSystemDescription("uimaTypes.vpdmf-digitalLibrary",
							"edu.isi.bmkeg.digitalLibrary.uima.TypeSystem");

			CollectionReader cr = CollectionReaderFactory
					.createCollectionReader(
							DigitalLibraryCollectionReader.class,
							typeSystem, DigitalLibraryCollectionReader.LOGIN,
							options.login,
							DigitalLibraryCollectionReader.PASSWORD,
							options.password,
							DigitalLibraryCollectionReader.DB_URL,
							options.dbName,
							DigitalLibraryCollectionReader.WORKING_DIRECTORY,
							options.workingDirectory,
							DigitalLibraryCollectionReader.CORPUS_NAME,
							options.corpus);

			AggregateBuilder builder = new AggregateBuilder();

			builder.add(SentenceAnnotator.getDescription()); // Sentence
															// segmentation
			builder.add(TokenAnnotator.getDescription());  // Tokenization

			builder.add(AnalysisEngineFactory.createPrimitiveDescription(
					AddFragmentsAndCodes.class,
					AddFragmentsAndCodes.LOGIN, options.login, 
					AddFragmentsAndCodes.PASSWORD, options.password, 
					AddFragmentsAndCodes.DB_URL, options.dbName, 
					AddFragmentsAndCodes.WORKING_DIRECTORY, options.workingDirectory,
					AddFragmentsAndCodes.FRAGMENT_TYPE, options.frgType
					));
			
			builder.add(AnalysisEngineFactory.createPrimitiveDescription(
					AddBratAnnotations.class,
					AddBratAnnotations.BRAT_DATA_DIRECTORY, options.inBrat
					));
			
			// The simple document classification annotator
			String corpusName = options.corpus;

			corpusName = corpusName.replaceAll("\\s+", "_");
			corpusName = corpusName.replaceAll("\\/", "_");

			String filePath = options.out.getPath() + "/" + corpusName + "_annSummary.txt";

			builder.add(AnalysisEngineFactory.createPrimitiveDescription(
					SummarizeBratAnnotations.class,
					SummarizeBratAnnotations.PARAM_FILE_PATH, filePath));

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
