 package de.appsist.service.cds;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.vertx.java.core.Handler;
import org.vertx.java.core.http.HttpServerFileUpload;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.logging.impl.LoggerFactory;
import org.vertx.java.platform.Verticle;

import de.appsist.commons.misc.StatusSignalConfiguration;
import de.appsist.commons.misc.StatusSignalSender;

/**
 * Main verticle of the content delivery service.
 * The content deliver service handles content packages within the appsist framework.
 * @author simon.schwantzer(at)im-c.de
 */
public class MainVerticle extends Verticle {
	private static final Logger logger = LoggerFactory.getLogger(MainVerticle.class);
	
	private JsonObject config;
	private RouteMatcher routeMatcher;
	private LocalFileHandler localFileHandler;
	private StaticContentHandler staticContentHandler;
	private MetadataHandler metadataHandler;

	@Override
	public void start() {
		initializeConfiguration();
		
		List<String> deployedPackages;
		try {
			deployedPackages = localFileHandler.initializePackages();
			logger.info(deployedPackages.size() + " new content packages available.");
			localFileHandler.checkContentPackages();
		} catch (IOException e) {
			logger.warn("Failed to deploy content packages.", e);
		}
		
		
		initializeHTTPRouting();
		vertx.createHttpServer()
			.requestHandler(routeMatcher)
			.listen(config.getObject("webserver")
			.getInteger("port"));
		
		JsonObject statusSignalObject = config.getObject("statusSignal");
		StatusSignalConfiguration statusSignalConfig;
		if (statusSignalObject != null) {
		  statusSignalConfig = new StatusSignalConfiguration(statusSignalObject);
		} else {
		  statusSignalConfig = new StatusSignalConfiguration();
		}

		StatusSignalSender statusSignalSender =
		  new StatusSignalSender("cds", vertx, statusSignalConfig);
		statusSignalSender.start();

		
		logger.debug("APPsist service \"Content Delivery Service\" has been initialized with the following configuration:\n" + config.encodePrettily());
	}
	
	/**
	 * Initializes the verticle configuration.
	 */
	private void initializeConfiguration() {
		if (container.config() != null && container.config().size() > 0) {
			config = container.config();
		} else {
			logger.warn("Warning: No configuration applied! Using default settings.");
			config = getDefaultConfiguration();
		}
		
		// Initialize directory for content packages. For this directory, write access is required.
		String contentPath = config.getString("contentPath");
		if (contentPath != null) {
			File f = new File(contentPath);
			if (f.isDirectory() && f.canWrite()) {
				localFileHandler = new LocalFileHandler(contentPath, vertx);
			} else {
				logger.warn("The given content directory cannot be accessed: " + contentPath + ". Please ensure the directory exists and is writable.");
			}
		} else {
			logger.warn("No content path configured. Local files will not be delivered.");
		}
		
		// Initialize static content directory. For this directory, read access is required.
		String staticContentPath = config.getString("staticContentPath");
		if (staticContentPath != null) {
			File f = new File(staticContentPath);
			if (f.isDirectory() && f.canRead()) {
				staticContentHandler = new StaticContentHandler(staticContentPath);
			} else {
				logger.warn("The given directory for static content cannot be accessed: " + staticContentPath + ". Please ensure the directory exists and is readable.");
			}
		} else {
			logger.warn("No path for static content configured.");
		}
		
		metadataHandler = new MetadataHandler();
	}
	
	@Override
	public void stop() {
		logger.debug("APPsist service \"Content Delivery Service\" has been stopped.");
	}
	
	/**
	 * Create a configuration which used if no configuration is passed to the module.
	 * @return Configuration object.
	 */
	private static JsonObject getDefaultConfiguration() {
		JsonObject defaultConfig =  new JsonObject();
		JsonObject webserverConfig = new JsonObject();
		webserverConfig.putNumber("port", 8080);
		webserverConfig.putString("basePath", "");
		defaultConfig.putObject("webserver", webserverConfig);
		return defaultConfig;
	}
	
	/**
	 * Routing of HTTP requests. 
	 */
	private void initializeHTTPRouting() {
		final String basePath = config.getObject("webserver").getString("basePath");
		routeMatcher = new BasePathRouteMatcher(basePath);
		final WebUIHandler webUiHandler = new WebUIHandler(basePath, localFileHandler);
		
		routeMatcher.getWithRegEx("/www/.*", new Handler<HttpServerRequest>() {
			@Override
			public void handle(HttpServerRequest request) {
				String filePath = request.path().substring(basePath.length() + 1);
				webUiHandler.resolveStaticFileRequest(request.response(), filePath);;
			}
		});
		
		routeMatcher.get("/overview", new Handler<HttpServerRequest>() {
			@Override
			public void handle(HttpServerRequest request) {
				String successId = request.params().get("success");
				webUiHandler.resolveOverviewRequest(request.response(), successId);
			}
		});
		
		routeMatcher.post("/upload", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest request) {
				final String contentId = request.params().get("contentId");
				request.expectMultiPart(true);
				request.uploadHandler(new Handler<HttpServerFileUpload>() {
					
					@Override
					public void handle(HttpServerFileUpload upload) {
						localFileHandler.resolveContentPackageUpload(request.response(), upload, contentId, basePath + "/overview?success=" + contentId);
					}
				});
			}
		});

		routeMatcher.get("/:id", new Handler<HttpServerRequest>() {
			@Override
			public void handle(HttpServerRequest request) {
				String contentId = request.params().get("id");
				boolean isMetadataRequest = request.params().contains("metadata");
				if (!isMetadataRequest) {
					localFileHandler.resolveContentPackageRequest(request.response(), contentId);
				} else {
					metadataHandler.resolveMetadataRequest(request.response(), contentId);
				}
			}
		});
		
		routeMatcher.delete("/:id", new Handler<HttpServerRequest>() {
			@Override
			public void handle(HttpServerRequest request) {
				String contentId = request.params().get("id");
				localFileHandler.resolveContentPackageDeletion(request.response(), contentId);
			}
		});
		
		// Requests for package independent content.  
		routeMatcher.getWithRegEx("/static/.+", new Handler<HttpServerRequest>() {
			
			@Override
			public void handle(HttpServerRequest request) {
				String path = request.path().substring(basePath.length() + 8);						
				staticContentHandler.resolveStaticContentRequest(request.response(), path);
			}
		});
		
		// Request for a file in a content package.
		routeMatcher.getWithRegEx("\\/([^\\/]+)\\/.+", new Handler<HttpServerRequest>() {
			
			@Override
			public void handle(HttpServerRequest request) {
				String contentId = request.params().get("param0");
				String path = request.path().substring(basePath.length() + contentId.length() + 2);						
				localFileHandler.resolveFileRequest(request.response(), contentId, path);
			}
		});
		
	}
}
