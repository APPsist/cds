package de.appsist.service.cds;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.http.HttpServerFileUpload;
import org.vertx.java.core.http.HttpServerResponse;
import org.vertx.java.core.json.DecodeException;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.logging.impl.LoggerFactory;

/**
 * Helper class for storing and delivering files from the local filesystem. 
 * @author simon.schwantzer(at)im-c.de
 */
public class LocalFileHandler {
	private static final Logger logger = LoggerFactory.getLogger(LocalFileHandler.class);
	
	private static final Pattern FILENAME_PATTERN = Pattern.compile("[a-z-A-Z_0-9\\.]+");
	
	private final String contentDir;
	private final Vertx vertx;
	
	public LocalFileHandler(String contentDir, Vertx vertx) {
		this.contentDir = contentDir;
		this.vertx = vertx;
	}

	/**
	 * Resolves a request for a specific file of a content package.
	 * If the requested file does not exist, a 404 response will be created. 
	 * @param response HTTP server response to send file.
	 * @param contentId ID of the content package the file is located in.
	 * @param path Path of the file in the content package.
	 */
	public void resolveFileRequest(HttpServerResponse response, String contentId, String path) {
		StringBuilder builder = new StringBuilder(400);
		builder.append(contentDir);
		builder.append("/").append(contentId).append("/").append(path);
		response.sendFile(builder.toString());
	}
	
	/**
	 * Resolves a request for a content package by returning the related zip file.
	 * @param response HTTP server response to send content package. 
	 * @param contentId ID of the content package requested.
	 */
	public void resolveContentPackageRequest(HttpServerResponse response, String contentId) {
		StringBuilder builder = new StringBuilder(400);
		builder.append(contentDir);
		builder.append("/").append(contentId).append(".zip");
		response.sendFile(builder.toString());
	}
	
	/**
	 * Resolves a content package upload by storing and unpacking the uploaded zip file.
	 * Existing content packages with the same ID will be overwritten. 
	 * @param response HTTP server response to send process information.
	 * @param upload Upload to handle.
	 * @param contentId ID of the content package to be stored.
	 * @param redirectUrl URL to redirect if the process succeeds.
	 */
	public void resolveContentPackageUpload(final HttpServerResponse response, HttpServerFileUpload upload, final String contentId, final String redirectUrl) {
		StringBuilder builder = new StringBuilder(400);
		builder.append(contentDir);
		builder.append("/").append(contentId).append(".zip");
		final String zipFilePath = builder.toString();
		upload.streamToFileSystem(zipFilePath).endHandler(new Handler<Void>() {
			
			@Override
			public void handle(Void event) {
				StringBuilder builder = new StringBuilder(400);
				builder.append(contentDir).append("/").append(contentId);
				String targetDir = builder.toString();
				
				try {
					Path targetPath = Paths.get(targetDir);
					if (Files.exists(targetPath)) {
						removeRecursive(targetPath);
					}
					ZipUtil.unzip(zipFilePath, targetDir);
					response.headers().set("Location", redirectUrl);
					response.setStatusCode(303);
					response.end();
				} catch (IOException e) {
					response.setStatusCode(415);
					response.end("Failed to extract content: " + e.getMessage());
				}
			}
		});		
	}
	
	/**
	 * Resolve a request for the deletion of a content package.
	 * Both directory and related zip file will be removed.
	 * @param response Response to inform about progress.
	 * @param contentId ID of the content package to delete.
	 */
	public void resolveContentPackageDeletion(HttpServerResponse response, String contentId) {
		StringBuilder builder = new StringBuilder(400);
		builder.append(contentDir).append("/").append(contentId);
		String contentDir = builder.toString();
		Path contentDirPath = Paths.get(contentDir);
		builder.append(".zip");
		String contentZip = builder.toString();
		Path contentZipPath = Paths.get(contentZip);
		try {
			boolean exists = Files.deleteIfExists(contentZipPath);
			if (exists) {
				removeRecursive(contentDirPath);
				response.end();
			} else {
				response.setStatusCode(404);
				response.end("No content package with id " + contentId + " available.");
			}
		} catch (IOException e) {
			response.setStatusCode(500);
			response.end("Failed to delete content package: " + e.getMessage());
		}
	}
	
	/**
	 * Returns a list of all content packages locally available.
	 * @return List of content package IDs.
	 */
	public List<String> getLocalContentPackageList() {
		List<String> contentIds = new ArrayList<>();
		Path contentPath = Paths.get(contentDir);
		try {
			DirectoryStream<Path> dirStream = Files.newDirectoryStream(contentPath, new DirectoryStream.Filter<Path>() {

				@Override
				public boolean accept(Path entry) throws IOException {
					return Files.isDirectory(entry);
				}
			});
			for (Path path : dirStream) {
				contentIds.add(path.getFileName().toString());
			}
		} catch (IOException e) {
			logger.warn("Failed to retrieve content directory listing.", e);
		}
		return contentIds;
	}
	
	/**
	 * Recursively deletes an directory.
	 * @param path Path of the directory.
	 * @throws IOException Failed to delete directory.
	 */
	private static void removeRecursive(Path path) throws IOException {
		Files.walkFileTree(path, new FileVisitor<Path>() {
			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
				Files.delete(file);
				return FileVisitResult.CONTINUE;
			}
			
			@Override
			public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
				Files.delete(dir);
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
				throw new IOException("Failed to delete file: " + file.toString());
			}

	   });
	}
	
	/**
	 * Tries to unzip all compressed packages.
	 * @throws IOException Failed to access content directory.
	 */
	public List<String> initializePackages() throws IOException {
		List<String> importedContentIds = new ArrayList<>();
		Path contentPath = Paths.get(contentDir);
		try {
			DirectoryStream<Path> zipFileStream = Files.newDirectoryStream(contentPath, new DirectoryStream.Filter<Path>() {

				@Override
				public boolean accept(Path entry) throws IOException {
					return Files.isRegularFile(entry) && entry.toString().endsWith(".zip");
				}
			});
			for (Path path : zipFileStream) {
				// check if directory with name exists
				String pathString = path.toString();
				String expectedDir = pathString.substring(0, pathString.length() - 4); // remove ".zip"
				boolean dirExists = Files.isDirectory(Paths.get(expectedDir));
				if (!dirExists) {
					ZipUtil.unzip(pathString, expectedDir);
					logger.debug("Deployed new content package: " + expectedDir);
					importedContentIds.add(expectedDir);
				}
			}
		} catch (IOException e) {
			logger.warn("Failed to retrieve content directory listing.", e);
		}
		return importedContentIds;
	}
	
	public void checkContentPackages() throws IOException {
		Path contentPath = Paths.get(contentDir);
		DirectoryStream<Path> contentPackageDirs = Files.newDirectoryStream(contentPath, new DirectoryStream.Filter<Path>() {

			@Override
			public boolean accept(Path entry) throws IOException {
				return Files.isDirectory(entry);
			}
		});
		for (Path packagePath : contentPackageDirs) {
			if (checkDescriptor(packagePath) && checkPackageContent(packagePath)) {
				logger.debug("Content package validated: " + packagePath.getFileName());
			};
		}

	}
	
	private boolean checkDescriptor(final Path packagePath) {
		final String packageId = packagePath.getFileName().toString();
		Path contentDescriptorPath = packagePath.resolve("content.json");
		if (!Files.exists(contentDescriptorPath) || !Files.isReadable(contentDescriptorPath)) {
			logger.warn("Content package descriptor does not exist or cannot be accessed: " + contentDescriptorPath.toString());
			return false;
		}
		
		String contentString = vertx.fileSystem().readFileSync(contentDescriptorPath.toString()).toString();
		try {
			new JsonObject(contentString);
			return true;
		} catch (DecodeException e) {
			logger.warn("Failed to parse content package descriptor for content package: " + packageId);
			return false;
		}
		
	}
	
	public boolean checkPackageContent(final Path packagePath) throws IOException {
		final List<Path> files = new ArrayList<>();
		Files.walkFileTree(packagePath, new SimpleFileVisitor<Path>() {
			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
				if (!attrs.isDirectory()) {
					files.add(file);
				}
				return FileVisitResult.CONTINUE;
			}
		});
		String packageId = packagePath.getFileName().toString();
		int packagePathCount = packagePath.getNameCount();
		for (Path file : files) {
			Path relativeFilePath = file.subpath(packagePathCount, file.getNameCount());
			for (int i = 0; i < relativeFilePath.getNameCount(); i++) {
				String fileName = relativeFilePath.getName(i).toString();
				boolean matches = FILENAME_PATTERN.matcher(fileName).matches();
				if (!matches) {
					logger.warn("Invalid file name in package \"" + packageId + "\": " + relativeFilePath.toString());
					return false;
				}
			}
		}
		return true;
	}
}
