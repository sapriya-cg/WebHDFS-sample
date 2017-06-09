package webhdfs.client.impl;

import java.io.File;
import java.nio.file.Files;
import java.util.List;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;

import webhdfs.client.WebHdfsClient;
import webhdfs.client.http.requestexecution.HttpRequestExecutionEngine;
import webhdfs.client.http.responsehandler.CreateFileResponseHandler;
import webhdfs.client.http.responsehandler.DeleteFilesResponseHandler;
import webhdfs.client.http.responsehandler.HdfsFile;
import webhdfs.client.http.responsehandler.ListFilesResponseHandler;
import webhdfs.client.http.responsehandler.MakeDirectoryResponseHandler;

/**
 * 
 * Java Client application for WebHdfs (no authentication version)
 *
 */
public class WebHdfsClientImpl implements WebHdfsClient {

	private static final String hostServer = "localhost"; // replace with hdfs server url
	private static final String port = "50070";
	private static final String protocol = "webhdfs";
	private static final String version = "v1";
	private static final String opString = "?op=";
	private static final String forwardSlash = "/";
	
	private static final String listFilesOp = "LISTSTATUS";
	private static final String createFileOp = "CREATE";
	private static final String deleteFileOp = "DELETE";
	private static final String makeDirOp = "MKDIRS";
	
	public static void main(String[] args) throws Exception {
		WebHdfsClientImpl client = new WebHdfsClientImpl();
		String hostURL = hostServer + ":" + port + "/" + protocol + "/"
				+ version;
		String hdfsDirPath = "/myDir"; // replace with hdfs path

		String hdfsFileName = "dataFile.txt";

		String localFilepath = "/dataFile.txt"; // replace with local file path

		// make directory
		client.makeDirectory(hostURL, hdfsDirPath);

		// create and upload hdfs file
		client.createFile(hostURL, hdfsDirPath + forwardSlash, hdfsFileName,
				localFilepath);

		// list file
		client.listFiles(hostURL, hdfsDirPath);

		// delete file
		String hdfsFilepath = hdfsDirPath + forwardSlash + hdfsFileName;
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

		MakeDirectoryResponseHandler makeDirResponseHandler = new MakeDirectoryResponseHandler();
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

		CreateFileResponseHandler createFileResponseHandler = new CreateFileResponseHandler();
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

}
