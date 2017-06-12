package webhdfs.client.impl;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.util.List;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;

import webhdfs.client.WebHdfsClient;
import webhdfs.client.http.requestexecution.HttpRequestExecutionEngine;
import webhdfs.client.http.responsehandler.CreateAndAppendFileResponseHandler;
import webhdfs.client.http.responsehandler.DeleteFilesResponseHandler;
import webhdfs.client.http.responsehandler.HdfsFile;
import webhdfs.client.http.responsehandler.ListFilesResponseHandler;
import webhdfs.client.http.responsehandler.MakeDirectoryAndRenameResponseHandler;
import webhdfs.client.http.responsehandler.ReadFileResponseHandler;

/**
 * 
 * Java Client application for WebHdfs (no authentication version)
 *
 */
public class WebHdfsClientImpl implements WebHdfsClient {

	private static final String hostServer = "localhost"; // replace with hdfs
															// server url

	private static final String port = "50070";
	private static final String protocol = "webhdfs";
	private static final String version = "v1";
	private static final String opString = "?op=";
	private static final String forwardSlash = "/";
	private static final String andOp = "&";

	private static final String listFilesOp = "LISTSTATUS";
	private static final String createFileOp = "CREATE";
	private static final String appendFileOp = "APPEND";
	private static final String deleteFileOp = "DELETE";
	private static final String makeDirOp = "MKDIRS";
	private static final String renameFileOp = "RENAME";
	private static final String readOp = "OPEN";

	public static void main(String[] args) throws Exception {
		WebHdfsClient client = new WebHdfsClientImpl();

		String hostURL = hostServer + ":" + port + "/" + protocol + "/"
				+ version;
		String hdfsDirPath = "/myHDFSDir"; // replace with hdfs path

		String hdfsFileName = "dataFile.txt";

		String localFilepath = "dataFile.txt"; // replace with local file path

		// make directory
		client.makeDirectory(hostURL, hdfsDirPath);

		// create and upload hdfs file
		client.createFile(hostURL, hdfsDirPath + forwardSlash, hdfsFileName,
				localFilepath);

		// append additional data to hdfs file
		client.appendFile(hostURL, hdfsDirPath + forwardSlash, hdfsFileName,
				localFilepath);

		// read back hdfs file
		String localReadFilepath = "dataFileRead.txt"; // replace with local
														// file path
		client.readFile(hostURL, hdfsDirPath + forwardSlash + hdfsFileName,
				localReadFilepath);

		// list file
		client.listFiles(hostURL, hdfsDirPath);

		// rename hdfs file / directory
		String newHdfsFileName = "NewDataFile.txt"; // replace with new file
													// name
		client.renameFile(hostURL, hdfsDirPath + forwardSlash + hdfsFileName,
				hdfsDirPath + forwardSlash + newHdfsFileName);

		// list file
		client.listFiles(hostURL, hdfsDirPath);

		// delete file
		String hdfsFilepath = hdfsDirPath + forwardSlash + newHdfsFileName;
		client.deleteFile(hostURL, hdfsFilepath);

		// list file
		client.listFiles(hostURL, hdfsDirPath);

		// remove directory
		client.removeDirectory(hostURL, hdfsDirPath);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see webhdfs.client.WebHdfsClient#removeDirectory(java.lang.String,
	 * java.lang.String)
	 */
	public void removeDirectory(String hostURL, String hdfsDirPath)
			throws Exception {
		deleteFile(hostURL, hdfsDirPath);

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see webhdfs.client.WebHdfsClient#makeDirectory(java.lang.String,
	 * java.lang.String)
	 */
	public void makeDirectory(String hostURL, String hdfsDirPath)
			throws Exception {
		HttpPut makeDirectoryRequest = new HttpPut(hostURL + hdfsDirPath
				+ opString + makeDirOp);

		HttpRequestExecutionEngine requestExecutor = new HttpRequestExecutionEngine();
		HttpResponse response = requestExecutor
				.executeRequest(makeDirectoryRequest);

		MakeDirectoryAndRenameResponseHandler makeDirResponseHandler = new MakeDirectoryAndRenameResponseHandler();
		System.out.println("Created: "
				+ makeDirResponseHandler.handleResponse(response));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see webhdfs.client.WebHdfsClient#deleteFile(java.lang.String,
	 * java.lang.String)
	 */
	public void deleteFile(String hostURL, String hdfsFilepath)
			throws Exception {
		HttpDelete deleteFilesRequest = new HttpDelete(hostURL + hdfsFilepath
				+ opString + deleteFileOp);

		HttpRequestExecutionEngine requestExecutor = new HttpRequestExecutionEngine();
		HttpResponse response = requestExecutor
				.executeRequest(deleteFilesRequest);

		DeleteFilesResponseHandler deleteFileResponseHandler = new DeleteFilesResponseHandler();
		System.out.println("Removed: "
				+ deleteFileResponseHandler.handleResponse(response));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see webhdfs.client.WebHdfsClient#listFiles(java.lang.String,
	 * java.lang.String)
	 */
	public void listFiles(String hostURL, String dirPath) throws Exception {
		HttpGet getFilesRequest = new HttpGet(hostURL + dirPath + opString
				+ listFilesOp);

		HttpRequestExecutionEngine requestExecutor = new HttpRequestExecutionEngine();
		HttpResponse response = requestExecutor.executeRequest(getFilesRequest);

		ListFilesResponseHandler listFilesResponseHandler = new ListFilesResponseHandler();
		List<HdfsFile> hdfsFiles = listFilesResponseHandler
				.handleResponse(response);

		for (HdfsFile hdfsFile : hdfsFiles) {
			System.out.println(hdfsFile.getFileName() + ":"
					+ hdfsFile.getFileType());
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see webhdfs.client.WebHdfsClient#createFile(java.lang.String,
	 * java.lang.String, java.lang.String, java.lang.String)
	 */
	public void createFile(String hostURL, String dirPath, String fileName,
			String localFilePath) throws Exception {
		// step 1 : create file.
		HttpPut createFileRequest = new HttpPut(hostURL + dirPath + fileName
				+ opString + createFileOp);

		HttpRequestExecutionEngine requestExecutor = new HttpRequestExecutionEngine();
		HttpResponse response = requestExecutor
				.executeRequest(createFileRequest);

		CreateAndAppendFileResponseHandler createFileResponseHandler = new CreateAndAppendFileResponseHandler();
		String redirectDataNodeURL = createFileResponseHandler
				.handleResponse(response);

		// step2: upload local file to the data node path returned in redirect.
		HttpPut uploadFileRequest = new HttpPut(redirectDataNodeURL);
		uploadFileRequest.addHeader("Content-Type", "application/octet-stream");

		byte[] bFile = Files.readAllBytes(new File(localFilePath).toPath());
		ByteArrayEntity requestEntity = new ByteArrayEntity(bFile);
		uploadFileRequest.setEntity(requestEntity);

		HttpResponse uploadFileResponse = requestExecutor
				.executeRequest(uploadFileRequest);

		// datanode path where the file is written to.
		System.out.println(createFileResponseHandler
				.handleResponse(uploadFileResponse));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see webhdfs.client.WebHdfsClient#appendFile(java.lang.String,
	 * java.lang.String, java.lang.String, java.lang.String)
	 */
	public void appendFile(String hostURL, String dirPath, String fileName,
			String localFilePath) throws Exception {
		// step 1 : get redirect path for Hdfs file.
		HttpPost appendFileRequest = new HttpPost(hostURL + dirPath + fileName
				+ opString + appendFileOp);

		HttpRequestExecutionEngine requestExecutor = new HttpRequestExecutionEngine();
		HttpResponse response = requestExecutor
				.executeRequest(appendFileRequest);

		CreateAndAppendFileResponseHandler createFileResponseHandler = new CreateAndAppendFileResponseHandler();
		String redirectDataNodeURL = createFileResponseHandler
				.handleResponse(response);

		// step2: append local file content to the redirect data node path
		HttpPost uploadFileRequest = new HttpPost(redirectDataNodeURL);
		uploadFileRequest.addHeader("Content-Type", "application/octet-stream");

		byte[] bFile = Files.readAllBytes(new File(localFilePath).toPath());
		ByteArrayEntity requestEntity = new ByteArrayEntity(bFile);
		uploadFileRequest.setEntity(requestEntity);

		requestExecutor.executeRequest(uploadFileRequest);

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see webhdfs.client.WebHdfsClient#renameFile(java.lang.String,
	 * java.lang.String, java.lang.String)
	 */
	public void renameFile(String hostURL, String hdfsFilePath,
			String hdfsNewFilePath) throws Exception {
		HttpPut renameFileRequest = new HttpPut(hostURL + hdfsFilePath
				+ opString + renameFileOp + andOp + "destination="
				+ hdfsNewFilePath);

		HttpRequestExecutionEngine requestExecutor = new HttpRequestExecutionEngine();
		HttpResponse response = requestExecutor
				.executeRequest(renameFileRequest);

		MakeDirectoryAndRenameResponseHandler makeDirResponseHandler = new MakeDirectoryAndRenameResponseHandler();
		System.out.println("Moved / Renamed: "
				+ makeDirResponseHandler.handleResponse(response));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see webhdfs.client.WebHdfsClient#readFile(java.lang.String,
	 * java.lang.String, java.lang.String)
	 */
	public void readFile(String hostURL, String hdfsFilePath,
			String localFilePath) throws Exception {
		HttpGet readFileRequest = new HttpGet(hostURL + hdfsFilePath + opString
				+ readOp);

		HttpRequestExecutionEngine requestExecutor = new HttpRequestExecutionEngine();
		HttpResponse response = requestExecutor.executeRequest(readFileRequest);

		ReadFileResponseHandler readFileResponseHandler = new ReadFileResponseHandler();
		ByteArrayInputStream byteArrayStream = readFileResponseHandler
				.handleResponse(response);

		writeToFile(byteArrayStream, localFilePath);

	}

	private void writeToFile(ByteArrayInputStream byteArrayStream,
			String localFilePath) throws Exception {
		FileOutputStream output = new FileOutputStream(localFilePath);

		try {
			int DEFAULT_BUFFER_SIZE = 1024;
			byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
			int n = 0;

			n = byteArrayStream.read(buffer, 0, DEFAULT_BUFFER_SIZE);

			while (n >= 0) {
				output.write(buffer, 0, n);
				n = byteArrayStream.read(buffer, 0, DEFAULT_BUFFER_SIZE);
			}
		} catch (Exception e) {
			throw e;
		} finally {
			output.close();
		}
	}
}
