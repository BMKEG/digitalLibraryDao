package edu.isi.bmkeg.digitalLibrary.bin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;

import edu.isi.bmkeg.digitalLibrary.utils.pubmed.ESearcher;
import edu.isi.bmkeg.vpdmf.model.definitions.VPDMf;

public class ListPmidsFromMedlineQuery {

	public static String USAGE = "arguments: <queryString> ";

	private static Logger logger = Logger
			.getLogger(BuildCorpusFromMedlineQuery.class);

	private VPDMf top;

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		if (args.length != 1) {
			System.err.println(USAGE);
			System.exit(-1);
		}

		String queryString = args[0];

		ESearcher eSearcher = new ESearcher(queryString);
		int maxCount = eSearcher.getMaxCount();
		List<Integer> esearchIds = new ArrayList<Integer>();
		for (int i = 0; i < maxCount; i = i + 1000) {

			long t = System.currentTimeMillis();

			esearchIds.addAll(eSearcher.executeESearch(i, 1000));

			long deltaT = System.currentTimeMillis() - t;
			logger.info("esearch 1000 entries: " + deltaT / 1000.0 + " s\n");

			logger.info("wait 3 secs");
			Thread.sleep(3000);
		}

		Collections.sort(esearchIds);
		for (int id : esearchIds) {
			System.out.println(id);
		}

	}
}
