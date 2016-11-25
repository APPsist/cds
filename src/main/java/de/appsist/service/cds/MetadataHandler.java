package de.appsist.service.cds;

import org.vertx.java.core.http.HttpServerResponse;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.logging.impl.LoggerFactory;

/**
 * Handler for metadata requests.
 */
public class MetadataHandler {
	private static final Logger logger = LoggerFactory.getLogger(MetadataHandler.class);
	/**
	 * Creates the handler. 
	 * @param logger Logger for system information.
	 */
	public MetadataHandler() {
		// TODO Implement me.
	}
	
	/**
	 * Resolves a metadata request.
	 * @param response Response to send information.
	 * @param contentId ID of the content package to retrieve metadata information for.
	 */
	public void resolveMetadataRequest(HttpServerResponse response, String contentId) {
		// TODO Implement me.
		logger.info("Metadata requests are not implemented yet.");
		response.setStatusCode(501);
		response.end("Not implemented.");
	}
}
