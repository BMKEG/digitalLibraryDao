package edu.isi.bmkeg.digitalLibrary.dao.vpdmf;

import java.io.File;
import java.sql.SQLException;

import org.springframework.core.io.Resource;

import edu.isi.bmkeg.vpdmf.controller.VPDMfKnowledgeBaseBuilder;

// TODO move this class to the core VPDMf project
public class BuildDBBean {
	
	private Resource vpdmfArchivePath;

	private String login;
	
	private String password;
	
	private String uri;

	public Resource getVpdmfArchivePath() {
		return vpdmfArchivePath;
	}

	public void setVpdmfArchivePath(Resource vpdmfArchivePath) {
		this.vpdmfArchivePath = vpdmfArchivePath;
	}
	
	public String getLogin() {
		return login;
	}

	public void setLogin(String login) {
		this.login = login;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getUri() {
		return uri;
	}

	public void setUri(String dbName) {
		this.uri = dbName;
	}

	public void init() throws Exception {

		if (!vpdmfArchivePath.exists()) {
			System.out.println("VPDMf archive resource does not exist: " + vpdmfArchivePath);
//			// VPDMf archive does not exist. Attempt to generate one
//			buildVpdmfArchive();
			
		}

		File buildFileSheets = vpdmfArchivePath.getFile();
		VPDMfKnowledgeBaseBuilder builder = new VPDMfKnowledgeBaseBuilder(buildFileSheets, login, password, uri);
		try {
			builder.destroyDatabase(uri);
		} catch (SQLException sqlE) {		
		
			if( !sqlE.getMessage().contains("database doesn't exist") ) {
					throw (sqlE);
				}
			
		}
		
		builder.buildDatabaseFromArchive();		
		
	}
}
