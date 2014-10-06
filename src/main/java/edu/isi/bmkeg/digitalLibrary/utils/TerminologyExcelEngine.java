package edu.isi.bmkeg.digitalLibrary.utils;

import java.awt.Dimension;
import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.poi.hssf.record.SSTRecord;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFFont;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;

import edu.isi.bmkeg.people.model.Person;
import edu.isi.bmkeg.terminology.model.Ontology;
import edu.isi.bmkeg.terminology.model.Term;
import edu.isi.bmkeg.utils.Converters;
import edu.isi.bmkeg.utils.excel.ExcelEngine;

public class TerminologyExcelEngine extends ExcelEngine {

	Logger log = Logger.getLogger("edu.isi.bmkeg.ooevv.utils.OoevvExcelEngine");

	private File data;
	private Ontology ontology;

	private byte[] fileBlob;
	private String fileName;

	private short bStyle = HSSFCellStyle.BORDER_THIN;
	private short bStyle2 = HSSFCellStyle.BORDER_MEDIUM;
	private String currSheet;
	private int sheetnum;
	private String currentExptName;
	private SSTRecord sstrec;

	/*
	 * public java.io.File buildExcelWorkbook2() throws Exception { short
	 * rownum;
	 * 
	 * // create a new file File file = new File(this.dataDirectory.getPath() +
	 * "/" + art.getFilenameStem() + "-dump.xls"); FileOutputStream out = new
	 * FileOutputStream(file);
	 * 
	 * // create a new workbook wb = new HSSFWorkbook();
	 * 
	 * cs = wb.createCellStyle(); f = wb.createFont();
	 * f.setBoldweight(HSSFFont.BOLDWEIGHT_BOLD); cs.setFont(f);
	 * 
	 * sCount = 0;
	 * 
	 * Iterator it = art.getExperiments().keySet().iterator(); while
	 * (it.hasNext()) { KefedModel expt = (KefedModel)
	 * art.getExperiments().get(it.next()); buildDataSheetsForExpt(expt);
	 * buildRelationSheetsForExpt(expt); buildCorrelationSheetsForExpt(expt); }
	 * 
	 * // write the workbook to the output stream // close our file (don't blow
	 * out our file handles wb.write(out); out.close();
	 * 
	 * return file;
	 * 
	 * }
	 */

	public void generateBlankOoevvExcelWorkbook(File file) throws Exception {

		FileOutputStream out = new FileOutputStream(file);

		// create a new workbook
		HSSFWorkbook wb = new HSSFWorkbook();
		setWb(wb);

		setCs(wb.createCellStyle());
		setF(wb.createFont());
		getF().setBoldweight(HSSFFont.BOLDWEIGHT_BOLD);
		getCs().setFont(getF());

		// ____________________________________________
		// Ontology
		//
		HSSFSheet s = wb.createSheet();
		wb.setSheetName(0, "Ontology");

		// declare a row object reference
		HSSFRow r = null;
		// declare a cell object reference
		HSSFCell c = null;

		r = s.createRow(0);
		c = r.createCell((short) 0);
		c.setCellStyle(getCs());
		c.setCellValue("name");

		r = s.createRow(1);
		c = r.createCell((short) 0);
		c.setCellStyle(getCs());
		c.setCellValue("description");

		r = s.createRow(2);
		c = r.createCell((short) 0);
		c.setCellStyle(getCs());
		c.setCellValue("prefix");

		r = s.createRow(3);
		c = r.createCell((short) 0);
		c.setCellStyle(getCs());
		c.setCellValue("namespace");

		// ____________________________________________
		// Terminology
		//
		s = wb.createSheet();
		wb.setSheetName(1, "Terminology");

		// Insert variable headings
		short rCount = 0;
		r = s.createRow(rCount);
		c = r.createCell((short) 0);
		c.setCellStyle(getCs());

		// write the top row of the data table.
		// localDefinition termValue shortTermId Stem fullTermURI definition
		// ontology
		r = s.createRow(rCount);

		c = r.createCell((short) 0);
		c.setCellStyle(getCs());
		c.setCellValue("localDefinition");

		c = r.createCell((short) 1);
		c.setCellStyle(getCs());
		c.setCellValue("termValue");

		c = r.createCell((short) 2);
		c.setCellStyle(getCs());
		c.setCellValue("shortTermId");

		c = r.createCell((short) 3);
		c.setCellStyle(getCs());
		c.setCellValue("definition");


		wb.write(out);

	}

	public HSSFWorkbook generateTerminologyExcelWorkbook(Ontology ont)
			throws Exception {

		this.ontology = ont;

		// create a new workbook
		HSSFWorkbook wb = new HSSFWorkbook();
		setWb(wb);

		setCs(wb.createCellStyle());
		setF(wb.createFont());
		getF().setBoldweight(HSSFFont.BOLDWEIGHT_BOLD);
		getCs().setFont(getF());

		// ____________________________________________
		// Ontology
		//
		HSSFSheet s = wb.createSheet();
		wb.setSheetName(0, "Ontology");

		// declare a row object reference
		HSSFRow r = null;
		// declare a cell object reference
		HSSFCell c = null;

		r = s.createRow(0);
		c = r.createCell((short) 0);
		c.setCellStyle(getCs());
		c.setCellValue("name");

		c = r.createCell((short) 1);
		c.setCellValue(ont.getFullName());
		
		r = s.createRow(1);
		c = r.createCell((short) 0);
		c.setCellStyle(getCs());
		c.setCellValue("description");

		c = r.createCell((short) 1);
		c.setCellValue(ont.getDescription());
		c = r.createCell((short) 1);
		c.setCellValue(ont.getShortName());

		r = s.createRow(2);
		c = r.createCell((short) 0);
		c.setCellStyle(getCs());
		c.setCellValue("prefix");

		r = s.createRow(3);
		c = r.createCell((short) 0);
		c.setCellStyle(getCs());
		c.setCellValue("namespace");

		c = r.createCell((short) 1);
		c.setCellValue(ont.getNamespace());

		// ____________________________________________
		// Terminology
		//
		s = wb.createSheet();
		wb.setSheetName(1, "Terminology");

		// Insert variable headings
		short rCount = 0;
		r = s.createRow(rCount);
		c = r.createCell((short) 0);
		c.setCellStyle(getCs());

		// write the top row of the data table.
		// localDefinition termValue shortTermId Stem fullTermURI definition
		// ontology
		r = s.createRow(rCount);

		c = r.createCell((short) 0);
		c.setCellStyle(getCs());
		c.setCellValue("localDefinition");

		c = r.createCell((short) 1);
		c.setCellStyle(getCs());
		c.setCellValue("termValue");

		c = r.createCell((short) 2);
		c.setCellStyle(getCs());
		c.setCellValue("shortTermId");

		c = r.createCell((short) 3);
		c.setCellStyle(getCs());
		c.setCellValue("definition");

		for(Term t : ont.getTerm()) {
			
			rCount++;
			r = s.createRow(rCount);

			c = r.createCell((short) 0);
			c.setCellValue(t.getLocalDefinition());

			c = r.createCell((short) 1);
			c.setCellValue(t.getTermValue());

			c = r.createCell((short) 2);
			c.setCellValue(t.getShortTermId());

			c = r.createCell((short) 3);
			c.setCellValue(t.getDefinition());

		}

		return getWb();

	}

	public Ontology createOntologyFromExcel()
			throws Exception {

		// ____________________________________________

		String sSheet = "Ontology";

		// detect row headings
		Map<String, Integer> rh = getRowHeadings(sSheet);

		String name = this.getData(0, 1, sSheet);
		String desc = this.getData(1, 1, sSheet);
		String prefix = this.getData(2, 1, sSheet);
		String namespace = this.getData(3, 1, sSheet);

		this.ontology = new Ontology();
		this.ontology.setFullName(name);
		this.ontology.setDescription(desc);
		this.ontology.setShortName(prefix);
		this.ontology.setNs(namespace);

		// ____________________________________________

		String vSheet = "Terminology";
		Dimension vDim = this.getMatrixDimensions(vSheet);
		Map<String, Integer> ch = getColumnHeadings(vSheet);
		int nValues = vDim.height - 1;

		for (int i = 0; i < nValues; i++) {

			Integer localDefCol = ch.get("localDefinition");
			Integer termValueCol = ch.get("termValue");
			Integer shortTermIdCol = ch.get("shortTermId");
			Integer defCol = ch.get("definition");

			if (localDefCol == null || termValueCol == null || shortTermIdCol == null
					|| defCol == null )
				throw new Exception("Misnamed Value Definition column headings");

			String localDef = this.getData(i + 1, localDefCol, vSheet);
			String termValue = this.getData(i + 1, termValueCol, vSheet);
			String shortTermId = this.getData(i + 1, shortTermIdCol, vSheet);
			String def = this.getData(i + 1, defCol, vSheet);

			// ____________________________________________
			// Read the spreadsheet and instantiate
			// OoEVV MeasurementValue objects appropriately
			//
			Term t = new Term();
			if( localDef.equals("1") || localDef.equals("true") ) {
				t.setLocalDefinition(true);				
			} else {
				t.setLocalDefinition(false);								
			}
			t.setTermValue(termValue);
			t.setShortTermId(shortTermId);
			t.setDefinition(def);
			
			t.setOntology(this.ontology);
			this.ontology.getTerm().add(t);
			
		}
		
		return this.ontology;

	}

}
