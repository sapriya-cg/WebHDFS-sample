package webhdfs.client.http.requestexecution;

import java.io.IOException;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.HttpClientBuilder;

/**
 * 
 * Common executor for all Http Requests.
 */
public class HttpRequestExecutionEngine {

	public HttpResponse executeRequest(HttpUriRequest request)
			throws ClientProtocolException, IOException {
		HttpClient httpClient = HttpClientBuilder.create().build();
		System.out.println("Executing Request :" + request.getURI());
		HttpResponse response = httpClient.execute(request);
		return response;
	}

}