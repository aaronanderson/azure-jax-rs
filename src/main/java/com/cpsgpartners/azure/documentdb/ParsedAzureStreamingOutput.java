package com.cpsgpartners.azure.documentdb;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.json.Json;
import javax.json.stream.JsonGenerator;
import javax.json.stream.JsonParser;
import javax.json.stream.JsonParser.Event;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

/*
 This utility class is used to stream the Azure DocumentDB REST response directly through to another REST client without buffering it in the intermediary server.
 This could be used for more advanced filtering
 */
public class ParsedAzureStreamingOutput implements StreamingOutput {

	Response response;

	public ParsedAzureStreamingOutput(Response response) {
		this.response = response;
	}

	@Override
	public void write(OutputStream os) throws IOException, WebApplicationException {
		final String continuationAzureToken = response.getHeaderString("x-ms-continuation");

		if (response.hasEntity()) {
			try (InputStream is = response.readEntity(InputStream.class)) {
				//Map<String, Object> properties = new HashMap<String, Object>(1);
				//properties.put(JsonGenerator.PRETTY_PRINTING, true);
				//JsonGeneratorFactory jgf = Json.createGeneratorFactory(properties);
				//JsonGenerator jg = jgf.createGenerator(os);
				JsonGenerator jg = Json.createGenerator(os);

				JsonParser jr = Json.createParser(is);

				//insert continuation at the beginning
				if (continuationAzureToken != null && jr.hasNext()) {
					Event event = jr.next();
					if (Event.START_OBJECT == event) {
						jg.writeStartObject();
						jg.write("_continuation", continuationAzureToken);
					}
				}

				// Clone the rest.
				String currentKey = null;
				while (jr.hasNext()) {
					Event event = jr.next();
					switch (event) {
					case END_ARRAY:
						jg.writeEnd();
						break;
					case END_OBJECT:
						jg.writeEnd();
						break;
					case KEY_NAME:
						currentKey = jr.getString();
						break;
					case START_ARRAY:
						if (currentKey != null) {
							jg.writeStartArray(currentKey);
							currentKey = null;
						} else
							jg.writeStartArray();
						break;
					case START_OBJECT:
						if (currentKey != null) {
							jg.writeStartObject(currentKey);
							currentKey = null;
						} else
							jg.writeStartObject();
						break;
					case VALUE_FALSE:
						if (currentKey != null) {
							jg.write(currentKey, false);
							currentKey = null;
						} else
							jg.write(false);
						break;
					case VALUE_NULL:
						if (currentKey != null) {
							jg.writeNull(currentKey);
							currentKey = null;
						} else
							jg.writeNull();
						break;
					case VALUE_NUMBER:
						if (currentKey != null) {
							jg.write(currentKey, jr.getBigDecimal());
							currentKey = null;
						} else
							jg.write(jr.getBigDecimal());
						break;
					case VALUE_STRING:
						if (currentKey != null) {
							//could filter here
							jg.write(currentKey, jr.getString());
							currentKey = null;
						} else
							jg.write(jr.getString());

						break;
					case VALUE_TRUE:
						if (currentKey != null) {
							jg.write(currentKey, true);
							currentKey = null;
						} else
							jg.write(true);

						break;
					default:
						break;

					}
				}

				/*//read the first, character, {
				if (is.read() != -1) {

					Writer writer = new BufferedWriter(new OutputStreamWriter(os));
					writer.write("{");

					if (continuationAzureToken != null) {
						writer.write("continuation: \"");
						writer.write(continuationAzureToken);
						writer.write("\",");
					}
					writer.flush();
					byte[] buffer = new byte[1024];
					int bytesRead;
					while ((bytesRead = is.read(buffer)) != -1) // test for EOF or end reached
					{
						os.write(buffer, 0, bytesRead);
					}
				}*/
			}
		}
		os.flush();
		os.close();
	}
}
