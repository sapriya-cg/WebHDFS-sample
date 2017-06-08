package webhdfs.client;

import java.io.File;
import java.nio.file.Files;
import java.util.List;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;

import webhdfs.client.http.requestexecution.HttpRequestExecutionEngine;
import webhdfs.client.http.responsehandler.CreateFileResponseHandler;
import webhdfs.client.http.responsehandler.ListFilesResponseHandler;

/**
 * 
 * Java Client application for WebHdfs.
 *
 */
public class WebHdfsClient {

	private static final String hostServer = "localhost"; //replace with hdfs server url
	private static final String port = "50070";
	private static final String protocol = "webhdfs";
	private static final String version = "v1";

	private static final String opString = "?op=";

	private static final String listFilesOp = "LISTSTATUS";
	private static final String createFileOp = "CREATE";

	public static void main(String[] args) throws Exception {
		WebHdfsClient client = new WebHdfsClient();
		String hostURL = hostServer + ":" + port + "/" + protocol + "/"
				+ version;
		String hdfsDirPath = "/mypath"; //replace with hdfs path

		String hdfsFileName = "myfile.txt";

		String localFilepath = "dataFile.txt"; //replace with local file path

		// create and upload hdfs file
		client.createFile(hostURL, hdfsDirPath, hdfsFileName, localFilepath);

		// list file
		client.listFiles(hostURL, hdfsDirPath);
	}

	/**
	 * To get list of Hdfs files in the hdfs server in the given directory path.
	 * 
	 * @param hostURL
	 *            Cannot be null
	 * @param dirPath
	 *            Cannot be null
	 * @throws Exception
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

	/**
	 * To create and upload Hdfs files in the hdfs server in the given directory
	 * path.
	 * 
	 * @param hostURL
	 *            Cannot be null
	 * @param dirPath
	 *            Cannot be null
	 * @param fileName
	 *            Cannot be null
	 * @param localFilePath
	 *            Cannot be null
	 * @throws Exception
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
