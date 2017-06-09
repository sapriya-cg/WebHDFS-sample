package webhdfs.client.http.responsehandler;

import java.io.IOException;

import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;

/**
 * To process create Hdfs file / append data to existing file Http response.
 *
 */
public class CreateAndAppendFileResponseHandler implements
		ResponseHandler<String> {

	/**
	 * Retrieves the data node URL (redirect URL) from HttpResponse. 
	 */
	public String handleResponse(HttpResponse response)
			throws ClientProtocolException, IOException {
		Header[] locationHeader = response.getHeaders("Location");
		return locationHeader[0].getValue();

	}

}