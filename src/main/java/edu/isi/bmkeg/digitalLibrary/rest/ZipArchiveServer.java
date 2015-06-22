package edu.isi.bmkeg.digitalLibrary.rest;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.io.Files;

import edu.isi.bmkeg.utils.Converters;
import edu.isi.bmkeg.vpdmf.dao.CoreDao;

@Controller
public class ZipArchiveServer {

	private static final Logger logger = Logger.getLogger(ZipArchiveServer.class);

	@Autowired
	private CoreDao coreDao;

	public void setCoreDao(CoreDao coreDao) {
		this.coreDao = coreDao;
	}
	
	@RequestMapping(value="/zipIt", method=RequestMethod.GET, params="corpus", produces=MediaType.APPLICATION_OCTET_STREAM_VALUE)
	public @ResponseBody ResponseEntity<byte []> byCorpusName(@RequestParam("corpus") String cName) throws Exception {
		
		HttpHeaders responseHeaders = new HttpHeaders();
		
		coreDao.connectToDb();
				
		File tempDir = Files.createTempDir();
		tempDir.deleteOnExit();
		String dAddr = tempDir.getAbsolutePath();
		cName = cName.replaceAll(" ", "_");
		Date date = new Date();
		String pattern = "yy-MM-dd-hhmm";
		SimpleDateFormat formatter = new SimpleDateFormat(pattern);
		String dateString = formatter.format(date);

		File tempFile = new File(dAddr + "/" + cName + "_" + dateString + "_pmcXml.zip");
		
		String wd = coreDao.getWorkingDirectory();

		String sql = "select ac.pmid, ftd.name " + 
				"from " + 
				"Corpus as c, " +
				"FTD as ftd, " +
				"LiteratureCitation as lc, " +
				"Corpus_corpora__resources_LiteratureCitation as c_lc, " +				
				"ArticleCitation as ac " +
				"where " +
				"lc.vpdmfId = ac.vpdmfId AND " + 
				"c_lc.resources_id = lc.vpdmfId AND " +
				"c_lc.corpora_id = c.vpdmfId AND " +
				"lc.fullText_id = ftd.vpdmfId AND " +
				"c.name = '" + cName + "';";

		ResultSet rs = coreDao.getCe().executeRawSqlQuery(sql);
		
		// Create a buffer for reading the files
		byte[] buf = new byte[1024];

		// Create the ZIP output stream for a binary file 
		// (don't want to load everything into memory)
		ZipOutputStream out = new ZipOutputStream(new FileOutputStream(
				tempFile));

		boolean fail = true;
		
		while( rs.next() ) {

			fail = false;
			Integer pmid = rs.getInt("ac.pmid");
			String pdfPath = rs.getString("ftd.name");
			String stemPath = wd + "/" + pdfPath.substring(0, pdfPath.lastIndexOf("."));
			File xml = new File(stemPath += ".nxml");
			
			if( xml.exists() ) {
				
				FileInputStream in = new FileInputStream(xml);

				// Add ZIP entry to output stream.
				out.putNextEntry(new ZipEntry(xml.getName()));

				// Transfer bytes from the file to the ZIP file
				int len;
				while ((len = in.read(buf)) > 0) {
					out.write(buf, 0, len);
				}

				// Complete the entry
				out.closeEntry();
				in.close();
			}
		
		}
		
		// Complete the ZIP file
		out.close();

		if(fail) {
			responseHeaders.add("Location", "http://bmkeg.isi.edu");
			return new ResponseEntity<byte []>(null, responseHeaders, HttpStatus.FOUND);
		}
		
		// Serialize zip to a byte[] object. 
		FileInputStream zipIs = new FileInputStream(tempFile);
		byte[] zipDat = IOUtils.toByteArray(zipIs);
		
		Converters.recursivelyDeleteFiles(tempDir);

		responseHeaders.setContentType(MediaType.valueOf("application/zip"));
	    responseHeaders.setContentLength(zipDat.length);
	    responseHeaders.set("Content-Disposition", "attachment;filename=\"" +  tempFile.getName() + '\"');

	    ResponseEntity<byte[]> response = new ResponseEntity<byte []>
        		(zipDat, responseHeaders, HttpStatus.OK);
        		
        return response;
		
	}
	
}
