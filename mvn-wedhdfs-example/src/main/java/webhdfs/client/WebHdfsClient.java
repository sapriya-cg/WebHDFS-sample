package webhdfs.client;

/**
 * 
 * Interface for WebHdfsClient.
 *
 */
public interface WebHdfsClient {
	/**
	 * Creates Hdfs directory.
	 * 
	 * @param hostURL
	 *            Cannot be null
	 * @param hdfsDirPath
	 *            Cannot be null
	 * @throws Exception
	 */
	public void makeDirectory(String hostURL, String hdfsDirPath)
			throws Exception;

	/**
	 * Removes Hdfs directory.
	 * 
	 * @param hostURL
	 *            Cannot be null
	 * @param hdfsDirPath
	 *            Cannot be null
	 * @throws Exception
	 */
	public void removeDirectory(String hostURL, String hdfsDirPath)
			throws Exception;

	/**
	 * Creates Hdfs files and uploads local file content to Hdfs file.
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
			String localFilePath) throws Exception;

	/**
	 * Deletes Hdfs file.
	 * 
	 * @param hostURL
	 *            Cannot be null
	 * @param hdfsFilepath
	 *            Cannot be null
	 * @throws Exception
	 */
	public void deleteFile(String hostURL, String hdfsFilepath)
			throws Exception;

	/**
	 * Lists files in the Hdfs directory.
	 * 
	 * @param hostURL
	 *            Cannot be null
	 * @param dirPath
	 *            Cannot be null
	 * @throws Exception
	 */
	public void listFiles(String hostURL, String dirPath) throws Exception;
}
