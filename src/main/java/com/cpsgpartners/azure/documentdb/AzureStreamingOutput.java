package com.cpsgpartners.azure.documentdb;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

/**
 This utility class is used to stream the Azure DocumentDB REST response directly through to another REST client without buffering it in the intermediary server.
 This class also has the additional option to convert the continuation token header into the JSON 
 
 for example,
 
 Response queryResult = db.queryDocuments(...
 return Response.ok(new AzureStreamingOutput(queryResult)).build();
 
 */
public class AzureStreamingOutput implements StreamingOutput {

	Response response;

	public AzureStreamingOutput(Response response) {
		this.response = response;
	}

	@Override
	public void write(OutputStream os) throws IOException, WebApplicationException {
		final String continuationAzureToken = response.getHeaderString("x-ms-continuation");

		if (response.hasEntity()) {
			try (InputStream is = response.readEntity(InputStream.class)) {
				if (continuationAzureToken != null) {
					//read the first, character, 
					if (is.read() != -1) {

						Writer writer = new BufferedWriter(new OutputStreamWriter(os));
						writer.write("{");

						if (continuationAzureToken != null) {
							writer.write("_continuation: \"");
							writer.write(continuationAzureToken);
							writer.write("\",");
						}
						writer.flush();

					}

				}
				byte[] buffer = new byte[1024];
				int bytesRead;
				while ((bytesRead = is.read(buffer)) != -1) // test for EOF or end reached
				{
					os.write(buffer, 0, bytesRead);
				}
			}
		}
		os.flush();
		os.close();
	}
}
