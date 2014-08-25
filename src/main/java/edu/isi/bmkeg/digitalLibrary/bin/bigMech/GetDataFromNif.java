package edu.isi.bmkeg.digitalLibrary.bin.bigMech;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;


/**
 * This command-line utility loads a file of OA PMC identifiers, loads their citations, 
 * downloads PDFs and XML data for each file
 * 
 * @author burns
 *
 */
public class GetDataFromNif {

	public Connection dbConnection;
	protected Set<String> drivers = new HashSet<String>();
	protected Statement stat;
	protected Statement uStat;
	
	public static class Options {

		@Option(name = "-url", usage = "Database ", required = false, metaVar = "PMC")
		public File pmcMapFile;

		@Option(name = "-l", usage = "Database login", required = true, metaVar = "LOGIN")
		public String login = "";

		@Option(name = "-p", usage = "Database password", required = true, metaVar = "PASSWD")
		public String password = "";

		@Option(name = "-db", usage = "Database name", required = true, metaVar = "DBNAME")
		public String dbName = "";

		@Option(name = "-wd", usage = "Working directory", required = true, metaVar = "WDIR")
		public String workingDirectory = "";

	}

	private static Logger logger = Logger.getLogger(GetDataFromNif.class);

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		Options options = new Options();
		Map<String,Integer> pmcIds = new HashMap<String,Integer>();
		Map<String,Integer> pmcIdMap = new HashMap<String,Integer>();
		Map<String,String> pdfLocs = new HashMap<String,String>();
		
		CmdLineParser parser = new CmdLineParser(options);
		
		try {
				

			Class.forName("org.postgresql.Driver").newInstance();

			String dir = "/Users/burns/Documents/Projects/5_planned/birnKarma/pilot/data";
			String uri = "jdbc:postgresql://nif-db.crbs.ucsd.edu:5432/disco_crawler";
			String user = "disco_reader";
			String pass = "rotemp123";
					
			// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
			// NOTE:
			// The 'useOldAliasMetadataBehavior=true' setting
			// is included to permit use of aliases within
			// ResultSetMetaData processing (which was changed since
			// VPDMf was originally developed).
			Connection dbConnection = DriverManager.getConnection(uri
					+ "?user=" + user + "&password=" + pass);

			if (dbConnection == null) {
				throw new Exception("Can't connect to db: " + uri);
			}
			
			Statement stat = dbConnection.createStatement(
					ResultSet.TYPE_FORWARD_ONLY, 
					ResultSet.CONCUR_READ_ONLY);
			
			String[] tables = {"l2_nif_0000_00093_brainmaps_connectivity",
					"l2_nif_0000_00093_brainmaps_terminology",
					"l2_nif_0000_00093_brainmaps_connectivity_reference",
					"l2_nif_0000_00018_bams_bams_id",
					"l2_nif_0000_00018_bams_swanson98",
					"l2_nif_0000_00018_bams_swanson98",
					"l2_nif_0000_24441_CONNECTOMEWIKI_NAME",
					"l2_nif_0000_24441_CONNECTOMEWIKI_FROM",
					"l2_nif_0000_24441_CONNECTOMEWIKI_NODE",
					"l2_nif_0000_24441_CONNECTOMEWIKI_TO",
					"l2_nif_0000_24805_data_list",
					"l2_nif_0000_00022_cocomac_brainsite",
					"l2_nif_0000_00022_cocomac_connectivity",
					"l2_nif_0000_00386_data_region",
					"l2_nif_0000_00386_data_connection"
			};
			
			for( int i=0; i<tables.length; i++) {
				
				String sql = "SELECT * FROM " + tables[i];
				ResultSet rs = stat.executeQuery(sql);
				ResultSetMetaData rsmd = rs.getMetaData();
				int columnCount = rsmd.getColumnCount();

				StringBuffer sb = new StringBuffer();
				
				// The column count starts from 1
				for (int j = 1; j < columnCount + 1; j++ ) {
					String name = rsmd.getColumnName(j);
					sb.append(name);
					sb.append("	");
				}
				
				sb.append("\n");
				
				while(rs.next()) {
					
					// The column count starts from 1
					for (int j = 1; j < columnCount + 1; j++ ) {
						String t = rsmd.getColumnTypeName(j);
						
						String s = "";
						if( t.equals("varchar") ) {
							s = rs.getString(j);
						} else if( t.startsWith("int")) {
							s = rs.getString(j);
						} else {
							s = "pause";
						}
		
						sb.append(s);							
						sb.append("	");
					}					
					sb.append("\n");
				}

				rs.close();
				
				String s = sb.toString();
				File out = new File(dir + "/" + tables[i] + ".txt");
				FileUtils.writeStringToFile(out, s);
				
			}
			
			logger.info("Finished");
			
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
