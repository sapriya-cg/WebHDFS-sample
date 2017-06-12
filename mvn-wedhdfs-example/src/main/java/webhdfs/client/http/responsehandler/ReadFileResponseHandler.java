package webhdfs.client.http.responsehandler;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;

public class ReadFileResponseHandler implements
		ResponseHandler<ByteArrayInputStream> {

	/**
	 * Extracts file data from response and return as ByteArray Ip stream.
	 */
	public ByteArrayInputStream handleResponse(HttpResponse response)
			throws ClientProtocolException, IOException {

		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		response.getEntity().writeTo(byteArrayOutputStream);

		return new ByteArrayInputStream(byteArrayOutputStream.toByteArray());

	}
}