package amp.source.youtube.base;

import java.lang.reflect.Field;
import java.net.URI;
import java.util.HashMap;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public abstract class SourceManagerStorage 
{
	private static final Logger cLogger = 
			LoggerFactory.getLogger(SourceManagerBean.class);
	
	protected String wkmStatusServiceURI = "";
				
	public String getWkmStatusServiceURI() {
		return wkmStatusServiceURI;
	}

	public void setWkmStatusServiceURI(String wkmStatusServiceURI) {
		this.wkmStatusServiceURI = wkmStatusServiceURI;
	}

	protected URI getStatusServiceURI() {
        return UriBuilder.fromUri(this.wkmStatusServiceURI).build();
	}
	
	//---
	public boolean setSourceItemStatus(String itemId, 
									   String sourceName,
									   String targetName,
									   String workerName,
									   String threadName,
									   String description, 
									   String status,
									   String opTypeName) 
	{
		String cMethodName = "";
		
		boolean cRes = true;
		
		try
		{
			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement ste = stacktrace[1];
	        cMethodName = ste.getMethodName();
	        
	        Client client = Client.create();

    		WebResource webResource = client.resource(this.getStatusServiceURI().toString());
    		
    		ClientResponse response = webResource.path("/setItemOpStatus").
					  queryParam("sourceName", String.valueOf(sourceName)).
					  queryParam("targetName", String.valueOf(targetName)).
					  queryParam("workerName", String.valueOf(workerName)).
					  queryParam("threadName", String.valueOf(threadName)).
					  queryParam("opTypeName", String.valueOf(opTypeName)).
					  queryParam("itemKey",    itemId).
					  queryParam("description", description). 
					  queryParam("status",     status).
					  accept(MediaType.APPLICATION_XML).
					  type(MediaType.APPLICATION_JSON).
					  post(ClientResponse.class);
    		
    		String cStatus = response.getEntity(String.class);
    		
    		if (response.getStatus() != 200) 
			{
    			String cResponse = String.valueOf(cStatus) + ":" + 
    							response.getEntity(String.class);
    			
    			System.out.println(cMethodName + "::" + cResponse);
    			
    			cLogger.error(cMethodName + "::" + cResponse);
			}
    		else
    		{
    			String cResponse = String.valueOf(cStatus) + ":" + 
    							response.getEntity(String.class);
    			
    			System.out.println(cMethodName + "::" + cResponse);
    			
    			cLogger.info(cMethodName + "::" + cResponse);
    		}
			
			return cRes;
    		
		}
		catch( Exception e)
		{
			System.out.println(cMethodName + "::Exception:" + e.getMessage());
			
			e.printStackTrace();
			
			return ( cRes = false );
		}
	}
	
	protected HashMap<String, Field> getInheritedFields(Class<?> type) 
	{
		String cMethodName = "";
		
		boolean cRes = true;
		
		HashMap<String, Field> cFields = new HashMap<String, Field>();
		
		try
		{
			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement ste = stacktrace[1];
	        cMethodName = ste.getMethodName();
	        
	        if ( type == null )
	        {
	        	cRes = false;
	        }
	        
	        if ( cRes )
	        {
		        for (Class<?> c = type; c != null; c = c.getSuperclass())
		        {
		        	for( Field cField : c.getDeclaredFields() )
		        	{
		        		cFields.put(cField.getName(), cField);
		        	}
		        }
	        }
	        
	        return cFields;
		}
		catch( Exception e)
		{
			System.out.println(cMethodName + "::Exception:" + e.getMessage());
			
			e.printStackTrace();
			
			return new HashMap<String, Field>();
		}
    }
}
