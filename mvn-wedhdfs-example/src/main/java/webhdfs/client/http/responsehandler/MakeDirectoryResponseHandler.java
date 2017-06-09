package webhdfs.client.http.responsehandler;

import java.io.IOException;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 * 
 * To process response of delete files request.
 *
 */
public class MakeDirectoryResponseHandler implements ResponseHandler<Boolean> {
	
	/**
	 * Retrieves True / False status from HttpResponse. 
	 */
	public Boolean handleResponse(HttpResponse response)
			throws ClientProtocolException, IOException {
		
		String json_string = EntityUtils.toString(response.getEntity());
		JSONObject jsonObj = (JSONObject) JSONValue.parse(json_string);
		
		return (Boolean) jsonObj.get("boolean");
		
	}

}
