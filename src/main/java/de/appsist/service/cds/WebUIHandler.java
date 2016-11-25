package de.appsist.service.cds;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.vertx.java.core.http.HttpServerResponse;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.logging.impl.LoggerFactory;

import com.github.jknack.handlebars.Handlebars;
import com.github.jknack.handlebars.Template;
import com.github.jknack.handlebars.io.ClassPathTemplateLoader;
import com.github.jknack.handlebars.io.TemplateLoader;

/**
 * Handler for web ui requets.
 * @author simon.schwantzer(at)im-c.de
 */
public class WebUIHandler {
	private static final Logger logger = LoggerFactory.getLogger(WebUIHandler.class);
	
	private final String basePath;
	private final LocalFileHandler localFileHandler;
	private Map<String, Template> templates; // Map with handlebars templates for HTML responses. 
	
	/**
	 * Creates the handler.
	 * @param basePath Base path for http requests. Is used templates are rendered.
	 * @param localFileHandler Handler to retrieve content list from.
	 * @param logger Logger for system information.
	 */
	public WebUIHandler(String basePath, LocalFileHandler localFileHandler) {
		this.basePath = basePath;
		this.localFileHandler = localFileHandler;
		templates = new HashMap<>();
		try {
			TemplateLoader loader = new ClassPathTemplateLoader();
			loader.setPrefix("/templates");
			loader.setSuffix(".html");
			Handlebars handlebars = new Handlebars(loader);
			templates.put("overview", handlebars.compile("overview"));
		} catch (IOException e) {
			logger.fatal("Failed to load templates.", e);
		}
	}
	
	/**
	 * Resolves a request for a static file. 
	 * @param response Response to send file.
	 * @param filePath Path of the file to deliver.
	 */
	public void resolveStaticFileRequest(HttpServerResponse response, String filePath) {
		response.sendFile(filePath);
	}
	
	public void resolveOverviewRequest(HttpServerResponse response, String successId) {
		JsonObject data = new JsonObject();
		data.putString("basePath", basePath);
		JsonArray contentIds = new JsonArray();
		for (String contentId : localFileHandler.getLocalContentPackageList()) {
			contentIds.addString(contentId);
			
		}
		data.putArray("contentIds", contentIds);
		data.putString("contentId", UUID.randomUUID().toString());
		if (successId != null) {
			data.putString("successId", successId);
		}
		try {
			String html = templates.get("overview").apply(data.toMap());
			response.end(html);
		} catch (IOException e) {
			response.setStatusCode(500); 
			response.end("Failed to render template.");
		}
	}
}
