package bdv.bigcat.viewer.atlas.opendialog;

import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.net.URL;

import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;

public class DVIDParser
{
	final static public < T > T fetch( final String url, final Type type ) throws JsonSyntaxException, JsonIOException, IOException
	{
		final Gson gson = new Gson();
		final T t = gson.fromJson( new InputStreamReader( new URL( url ).openStream() ), type );
		return t;
	}

}
