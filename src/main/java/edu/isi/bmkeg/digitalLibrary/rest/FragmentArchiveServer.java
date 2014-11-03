package edu.isi.bmkeg.digitalLibrary.rest;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
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

import edu.isi.bmkeg.digitalLibrary.model.qo.citations.LiteratureCitation_qo;
import edu.isi.bmkeg.ftd.model.qo.FTDFragment_qo;
import edu.isi.bmkeg.ftd.model.qo.FTD_qo;
import edu.isi.bmkeg.utils.Converters;
import edu.isi.bmkeg.vpdmf.dao.CoreDao;
import edu.isi.bmkeg.vpdmf.model.definitions.VPDMf;
import edu.isi.bmkeg.vpdmf.model.instances.LightViewInstance;

@Controller
public class FragmentArchiveServer {

	private static final Logger logger = Logger.getLogger(FragmentArchiveServer.class);

	@Autowired
	private CoreDao coreDao;

	public void setCoreDao(CoreDao coreDao) {
		this.coreDao = coreDao;
	}
	
	@RequestMapping(value="/fullText2CoNLLFormat", method=RequestMethod.GET, params="ftdId", produces=MediaType.APPLICATION_OCTET_STREAM_VALUE)
	public @ResponseBody ResponseEntity<byte[]> fullTextToCoNLLFormat(@RequestParam("ftdId") String ftdId) throws Exception {
		
		HttpHeaders responseHeaders = new HttpHeaders();
		
		coreDao.connectToDb();
				
		File tempDir = Files.createTempDir();
		tempDir.deleteOnExit();
		String dAddr = tempDir.getAbsolutePath();
		Date date = new Date();
		String pattern = "yy-MM-dd-hhmm";
		SimpleDateFormat formatter = new SimpleDateFormat(pattern);
		String dateString = formatter.format(date);

		String wd = coreDao.getWorkingDirectory();

		VPDMf top = coreDao.getTop();

		FTD_qo ftd = new FTD_qo();
		ftd.setVpdmfId(ftdId);
		LiteratureCitation_qo lc = new LiteratureCitation_qo();
		lc.setFullText(ftd);
		List<LightViewInstance> l = coreDao.list(lc, "ArticleCitationDocument");
			
		if( l.size() != 1 ) 
			throw new Exception("Can't find Full Text Document with id=" + ftdId);
			
		Map<String,String> idxMap = l.get(0).readIndexTupleMap(top);
		String authorString = idxMap.get("[ArticleCitation]Author|Person.surname");	
		String[] authorArray = authorString.split(LightViewInstance.INDEX_TUPLE_FIELD_SEPARATOR); 
		if( authorArray.length == 1 ) {
			authorArray = authorString.split(","); 
		}
		String author = authorArray[0];
		String year = idxMap.get("[ArticleCitation]LiteratureCitation|LiteratureCitation.pubYear");	
		String volume = idxMap.get("[ArticleCitation]LiteratureCitation|ArticleCitation.volume");	
		String pages = idxMap.get("[ArticleCitation]LiteratureCitation|LiteratureCitation.pages");
		pages = pages.substring(0,pages.indexOf("-"));
		String stem = author + "_" + year + "_" + volume + "_" + pages;

		File tempFile = new File(dAddr + "/" + stem + "_fragments.zip");

		FTDFragment_qo qFrg = new FTDFragment_qo();
		ftd = new FTD_qo();
		qFrg.setFtd(ftd);
		ftd.setVpdmfId(String.valueOf(ftdId));
		l = coreDao.list(qFrg, "FTDFragment");
				
		File frgDir = new File(tempDir.getPath() + "/" + stem);
		frgDir.mkdir();
		
		// Create a buffer for reading the files
		byte[] buf = new byte[1024];

		// Create the ZIP output stream for a binary file 
		// (don't want to load everything into memory)
		ZipOutputStream out = new ZipOutputStream(new FileOutputStream(
				tempFile));

		boolean fail = true;
		
		for( int i=0; i<l.size(); i++ ) {
			LightViewInstance lvi = l.get(i);
				
			fail = false;

			Map<String,String> idxMap2 = lvi.readIndexTupleMap(top);
			
			String frgType = idxMap2.get("[FTDFragment]FTDFragment|FTDFragment.frgOrder");
			if( frgType != null  && frgType.length() > 0 )
				frgType = frgType.replaceAll("\\s+", "_");
			else 
				frgType = "";

			String frgName = stem+"/"+stem+"_"+frgType+"_" + i + ".txt";
				
			String frgText = lvi.getVpdmfLabel();
			int pos = frgText.indexOf("[");
			frgText = frgText.substring(pos+1,frgText.length()-1);
			frgText = frgText.replaceAll("\\s+", " ");
			InputStream in = IOUtils.toInputStream(frgText, "UTF-8");
					
			// Add ZIP entry to output stream.
			out.putNextEntry(new ZipEntry(frgName));			
			
			// Transfer bytes from the file to the ZIP file
			int len;
			while ((len = in.read(buf)) > 0) {
				out.write(buf, 0, len);
			}

			// Complete the entry
			out.closeEntry();
		
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
		 
		responseHeaders.setContentType(MediaType.valueOf("application/zip"));
	    responseHeaders.setContentLength(zipDat.length);
	    responseHeaders.set("Content-Disposition", "attachment;filename=\"" +  tempFile.getName() + '\"');

	    ResponseEntity<byte[]> response = new ResponseEntity<byte []>
        		(zipDat, responseHeaders, HttpStatus.OK);
        
        Converters.recursivelyDeleteFiles(tempDir);
      		
        return response;
		
	}
	
}
