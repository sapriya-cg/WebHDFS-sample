package webhdfs.client.http.responsehandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 * To process ListFiles Http response.
 *
 */
public class ListFilesResponseHandler implements
		ResponseHandler<List<HdfsFile>> {

	/**
	 * Retrieves List of Hdfs Files from HttpResponse. 
	 */
	public List<HdfsFile> handleResponse(HttpResponse response)
			throws ClientProtocolException, IOException {
		String json_string = EntityUtils.toString(response.getEntity());
		JSONObject jsonObj = (JSONObject) JSONValue.parse(json_string);

		JSONObject fileStatusesJsonObj = (JSONObject) jsonObj.get("FileStatuses");
		
		JSONArray fileStatusArray = (JSONArray) fileStatusesJsonObj.get("FileStatus");
		Object[] fileStatusObjectArray = fileStatusArray.toArray();
		List<HdfsFile> hdfsFiles = new ArrayList<HdfsFile>();

		for (Object fileStatus : fileStatusObjectArray) {
			JSONObject fileStatusJsonObject = (JSONObject) fileStatus;

			String type = (String) fileStatusJsonObject.get("type");
			String name = (String) fileStatusJsonObject.get("pathSuffix");
			HdfsFile hdfsFile = new HdfsFile(name, type);

			hdfsFiles.add(hdfsFile);
		}

		return hdfsFiles;
	}

}