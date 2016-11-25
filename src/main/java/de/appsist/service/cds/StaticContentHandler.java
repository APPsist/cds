package de.appsist.service.cds;

import org.vertx.java.core.http.HttpServerResponse;

/**
 * Handler for static content requests.
 * Static content files are intended to be used accross content packages.  
 * @author simon.schwantzer(at)im-c.de
 *
 */
public class StaticContentHandler {
	public final String staticContentDir;
	
	public StaticContentHandler(String staticContentDir) {
		this.staticContentDir = staticContentDir;
	}
	
	/**
	 * Resolves a request for static content.
	 * If the requested file does not exist, a 404 response will be created. 
	 * @param response Response to send file content.
	 * @param path Path of the file requested.
	 */
	public void resolveStaticContentRequest(HttpServerResponse response, String path) {
		StringBuilder builder = new StringBuilder(400);
		builder.append(staticContentDir);
		builder.append("/").append(path);
		response.setChunked(false);
		response.sendFile(builder.toString());
	}

}
