package edu.isi.bmkeg.digitalLibrary.utils.pubmed;

import java.util.ArrayList;

import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;

import edu.isi.bmkeg.digitalLibrary.model.citations.ArticleCitation;
import edu.isi.bmkeg.digitalLibrary.model.citations.Author;
import edu.isi.bmkeg.digitalLibrary.model.citations.Journal;

class EfetchHandler extends DefaultHandler {

	ArticleCitation article;
	Author person;
	Journal journal;

	boolean error = false;

	private ArrayList<ArticleCitation> articles = new ArrayList<ArticleCitation>();
	int authorCount = 0;
	int urlCount = 0;
	int idCount = 0;
	int keyCount = 0;
	String currentMatch = "";
	String currentAttribute = "";
	int onePercent;

	int globalPosition = 0;
	int lastPosition = 0;

	ArrayList<Exception> exceptions = new ArrayList<Exception>();

	public void startDocument() {
		articles = new ArrayList<ArticleCitation>();
	}

	public void startElement(String uri, String localName, String qName,
			Attributes attributes) {

		this.currentMatch += "." + qName;
		this.currentAttribute = attributes.getValue("IdType");

		if (currentMatch.endsWith(".PubmedArticle")) {
			article = new ArticleCitation();
			articles.add(article);
			this.authorCount = 0;
			this.idCount = 0;
			this.keyCount = 0;
			this.urlCount = 0;
		}
		//
		// Parse the author information
		//
		else if (currentMatch.endsWith(".Author")) {

			try {
				person = new Author();
				article.getAuthorList().add(person);
			} catch (Exception e) {
				this.exceptions.add(e);
			}
		}
		/*
		 * else if( currentMatch.endsWith("ArticleId") &&
		 * this.currentAttribute.equals("pii") ) {
		 * 
		 * try { idCount = addLocalPrimitive( vi, "ID", idCount, "]ID|ID.id",
		 * "pii:", ""); } catch (Exception ex) { ex.printStackTrace(); }
		 * 
		 * } else if( currentMatch.endsWith( ".MeshHeading.DescriptorName" ) ) {
		 * 
		 * try { keyCount = addLocalPrimitive( vi, "Keyword", keyCount,
		 * "]Keyword|Keyword.value", "", ""); } catch (Exception ex) {
		 * ex.printStackTrace(); }
		 * 
		 * }
		 */

	}

	public void endElement(String uri, String localName, String qName) {
		String c = this.currentMatch;
		this.currentMatch = c.substring(0, c.lastIndexOf("." + qName));
	}

	public void characters(char[] ch, int start, int length) {
		String value = new String(ch, start, length);

		this.lastPosition = start;

		try {

			//
			// Parse the pubmed information
			//
			if (currentMatch.endsWith(".LastName")) {
				if( person.getSurname() != null )
					person.setSurname(person.getSurname() + " " + value);
				else 
					person.setSurname(value);
			} 
			else if (currentMatch.endsWith(".Initials")) {
				if( person.getInitials() != null )
					person.setInitials(value + " " + person.getInitials() );
				else 
					person.setInitials(value);
			}
			else if (currentMatch.endsWith(".CollectiveName")) {
				if( person.getSurname() != null )
					person.setSurname(value + " " + person.getSurname() );
				else 
					person.setSurname(value);
			}	
			//
			// Title information
			//
			else if (currentMatch.endsWith(".ArticleTitle")) {
				if( article.getTitle() != null )
					article.setTitle(article.getTitle() + value);
				else 
					article.setTitle(value);
			}
			//
			// Date information
			//
			else if (currentMatch.endsWith(".PubDate.Year")) {
				article.setPubYear(new Integer(value).intValue());
			}
			//
			// Date information
			//
			else if (currentMatch.endsWith(".PubDate.Year")) {
				article.setPubYear(new Integer(value).intValue());
			}
			//
			// Page information
			//
			else if (currentMatch.endsWith(".MedlinePgn")) {
				article.setPages(value);
			}
			//
			// Volume does not change
			else if (currentMatch.endsWith(".Volume")) {
				article.setVolume(value);
			}
			//
			// Issue does not change
			else if (currentMatch.endsWith(".Issue")) {
				article.setIssue(value);
			}
			//
			// Source (journal) doesn't change but must force a lookup in
			// the
			// relevent Lookup FormControl
			//
			else if (currentMatch.endsWith(".Article.Journal.ISOAbbreviation")) {
				if( article.getJournal() == null ) {
					journal = new Journal();
					article.setJournal(journal);
				}
				if( article.getJournal().getAbbr() != null )
					article.getJournal().setAbbr(article.getJournal().getAbbr() + value);
				else 
					article.getJournal().setAbbr(value);
				
			}
			else if (currentMatch.endsWith(".Article.Journal.Title")) {
				if( article.getJournal() == null ) {
					journal = new Journal();
					article.setJournal(journal);
				}
				if( article.getJournal().getJournalTitle() != null )
					article.getJournal().setJournalTitle(article.getJournal().getJournalTitle() + value);
				else 
					article.getJournal().setJournalTitle(value);
			}
			//
			// Source (journal) doesn't change but must force a lookup in
			// the relevent Lookup FormControl
			else if (currentMatch.endsWith(".AbstractText")) {
				if( article.getAbstractText() != null )
					article.setAbstractText(article.getAbstractText() + value);
				else 
					article.setAbstractText(value);
				
			}
			//
			//
			else if (currentMatch.endsWith(".PubmedArticle.MedlineCitation.PMID")) {
				article.setPmid(new Integer(value).intValue());
			}

		} catch (Exception e) {

			this.exceptions.add(e);

		}

	}
	
	public ArrayList<ArticleCitation> getArticles() {
		return this.articles;
	}

}
