package webhdfs.client.http.responsehandler;

/**
 * 
 * HDFS File definition, contains file name and file type.
 *
 */
public class HdfsFile {

	private String fileName;

	private String fileType;

	/**
	 * Returns hdfs file name
	 * 
	 * @return Not null
	 */
	public String getFileName() {
		return fileName;
	}

	/**
	 * Returns hdfs file type (file / directory)
	 * 
	 * @return Not null
	 */
	public String getFileType() {
		return fileType;
	}

	public HdfsFile(String name, String type) {
		this.fileName = name;
		this.fileType = type;
	}

}
