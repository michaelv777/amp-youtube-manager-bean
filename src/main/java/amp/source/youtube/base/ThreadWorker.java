/**
 * 
 */
package amp.source.youtube.base;

import java.net.URI;
import java.util.Calendar;
import java.util.HashMap;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriBuilder;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.watson.developer_cloud.http.HttpStatus;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

import amp.common.api.impl.ToolkitConstants;
import amp.jpa.entities.ThreadConfiguration;
import amp.jpa.entities.WorkerData;
import amp.jpaentities.mo.WorkerDataListMO;
import amp.jpaentities.mo.WorkerDataMO;
import amp.jpaentities.mo.WorkerThreadMO;

/**
 * @author MVEKSLER
 *
 */
public abstract class ThreadWorker 
{
	private static final Logger cLogger = 
			LoggerFactory.getLogger(ThreadWorker.class);

	protected URI getStatusBaseURI() {
        return UriBuilder.fromUri(this.wkStatusServiceURI).build();
	}
	
	protected URI getDataBaseURI() {
        return UriBuilder.fromUri(this.wkDataServiceURI).build();
	}
	
	protected boolean wkIsRunThread = true;
	
	protected String wkStatusServiceURI = "";
					
	protected String wkDataServiceURI = "";
	
	protected WorkerThreadMO cWorkerThreadMO = 
			new WorkerThreadMO();
	
	protected HashMap<String, ThreadConfiguration> 
		cThreadConfiguration = 
			new HashMap<String, ThreadConfiguration>();
	
	protected HashMap<String, String> 
		cThreadConfigurationMap = 
			new HashMap<String, String>();
	
	protected HashMap<String, String> 
		cSystemConfiguration = 
			new  HashMap<String, String>();
	
	protected boolean lcRes = true;
	
	//---getters/setters
	public boolean isLcRes() {
		return lcRes;
	}

	public HashMap<String, String> getcSystemConfiguration() {
		return cSystemConfiguration;
	}

	public void setcSystemConfiguration(HashMap<String, String> cSystemConfiguration) {
		this.cSystemConfiguration = cSystemConfiguration;
	}

	public WorkerThreadMO getcWorkerThreadMO() {
		return cWorkerThreadMO;
	}

	public void setcWorkerThreadMO(WorkerThreadMO cWorkerThreadMO) {
		this.cWorkerThreadMO = cWorkerThreadMO;
	}

	public HashMap<String, ThreadConfiguration> getcThreadConfiguration() {
		return cThreadConfiguration;
	}

	public void setcThreadConfiguration(HashMap<String, ThreadConfiguration> cThreadConfiguration) {
		this.cThreadConfiguration = cThreadConfiguration;
	}

	public HashMap<String, String> getcThreadConfigurationMap() {
		return cThreadConfigurationMap;
	}

	public void setcThreadConfigurationMap(HashMap<String, String> cThreadConfigurationMap) {
		this.cThreadConfigurationMap = cThreadConfigurationMap;
	}

	public boolean isWkIsRunThread() {
		return wkIsRunThread;
	}

	public void setWkIsRunThread(boolean wkIsRunThread) {
		this.wkIsRunThread = wkIsRunThread;
	}

	public String getWkStatusServiceURI() {
		return wkStatusServiceURI;
	}

	public void setWkStatusServiceURI(String wkStatusServiceURI) {
		this.wkStatusServiceURI = wkStatusServiceURI;
	}

	public String getWkDataServiceURI() {
		return wkDataServiceURI;
	}

	public void setWkDataServiceURI(String wkDataServiceURI) {
		this.wkDataServiceURI = wkDataServiceURI;
	}

	public void setLcRes(boolean lcRes) {
		this.lcRes = lcRes;
	}
	
	public ThreadWorker()
	{
		
	}
	
	protected boolean initClassVariables()
	{
		boolean cRes = true;
		
		String cMethodName = "";
		
		try
		{
			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement ste = stacktrace[1];
	        
	        cMethodName = ste.getMethodName();
	        
	        this.cThreadConfiguration = new HashMap<String, ThreadConfiguration>();
	        
	        this.cThreadConfigurationMap = new  HashMap<String, String>();
	        
	        this.cSystemConfiguration = new  HashMap<String, String>();

	        this.setLcRes(cRes);
		
	        return cRes;
		}
		catch( Exception e)
		{
			cLogger.error("M.V. Custom::" + cMethodName + "::" + e.getMessage());
			
			e.printStackTrace();
			
			this.setLcRes(cRes = false);
			
			return cRes;
		}
	}
	//---
	protected boolean saveWorkerData() 
	{
		String cMethodName = "";

		boolean cRes = true;
		
		try
		{
			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement ste = stacktrace[1];
	        cMethodName = ste.getMethodName();
	        
	       
			return cRes;
		}
		catch( Exception e)
		{
			cLogger.error("M.V. Custom::" + cMethodName + "::" + e.getMessage());
			
			e.printStackTrace();
			
			this.setLcRes(cRes = false);
			
			return cRes;
		}
	}
	
	protected WorkerDataListMO getItemOpStatus(
			   String sourceName,
			   String targetName,
			   String workerName,
			   String threadName,
			   String itemKey, 
		       String opTypeName) 
	{
		String cMethodName = "";

		WorkerDataListMO cWorkerDataListMO = new WorkerDataListMO();

		try 
		{
			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
			StackTraceElement ste = stacktrace[1];
			cMethodName = ste.getMethodName();

			// ---prepare error response
			WorkerData cWorkerDataErr = new WorkerData();
			cWorkerDataErr.setUpdatedate(Calendar.getInstance().getTime());
			cWorkerDataErr.setItemid(ToolkitConstants.ERROR_STR);

			WorkerDataMO cWorkerDataMOErr = new WorkerDataMO();
			cWorkerDataMOErr.setcWorkerData(cWorkerDataErr);

			cWorkerDataListMO.cWorkerData.add(cWorkerDataMOErr);
			// ---

			Client client = Client.create();

			WebResource webResource = client.resource(this.getStatusBaseURI().toString());

			ClientResponse response = webResource.path("/getItemOpStatus")
					.queryParam("sourceName", sourceName)
					.queryParam("targetName", targetName)
					.queryParam("workerName", workerName)
					.queryParam("threadName", threadName)
					.queryParam("opTypeName", opTypeName)
					.queryParam("itemKey", itemKey)
					.get(ClientResponse.class);

			if (response.getStatus() == HttpStatus.OK) 
			{
				cWorkerDataListMO = response.getEntity(WorkerDataListMO.class);

				for (WorkerDataMO cWorkerDataMO : cWorkerDataListMO.cWorkerData) 
				{
					WorkerData cWorkerData = cWorkerDataMO.cWorkerData;

					System.out.println(cMethodName + "::" + 
									   cWorkerData.getItemid() + ":" + 
									   cWorkerData.getOperationtypeM().getName() + ":" + 
									   cWorkerData.getUpdatedate().toString());
				}
			}

			return cWorkerDataListMO;
		} 
		catch (Exception e) 
		{
			System.out.println(cMethodName + "::Exception:" + e.getMessage());

			e.printStackTrace();

			return cWorkerDataListMO;
		}
	}
	
	//---
	protected boolean setItemOpStatus(String channelId,
									  String itemId, 
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
	        
	        String workerName  = this.getcWorkerThreadMO().getcWorker().getName();
	        String threadName  = this.getcWorkerThreadMO().getcThread().getName();
	        String sourceName  = ToolkitConstants.AMP_AMAZON_SOURCE;
	        String targetName  = ToolkitConstants.AMP_YOUTUBE_VIDEO_TARGET;
	        
	        if ( StringUtils.isEmpty(description))
	        {
	        	description = channelId + ":" + itemId;
	        }
	        
	        Client client = Client.create();

    		WebResource webResource = client.resource(this.getStatusBaseURI().toString());
    		
    		ClientResponse response = webResource.path("/setItemOpStatus").
					  queryParam("sourceName", sourceName).
					  queryParam("targetName", targetName).
					  queryParam("workerName", workerName).
					  queryParam("threadName", threadName).
					  queryParam("opTypeName", opTypeName).
					  queryParam("itemKey",    itemId).
					  queryParam("description", description). 
					  queryParam("status",      status).
					  accept(MediaType.APPLICATION_XML).
					  type(MediaType.APPLICATION_JSON).
					  post(ClientResponse.class);
    		
    		String cStatus = response.getEntity(String.class);
    		
    		if (response.getStatus() != 200) 
			{
    			System.out.println(cMethodName + "::" + cStatus);
    			
    			cLogger.error(cMethodName + "::" + cStatus);
			}
    		else
    		{
    			cLogger.info(cMethodName + "::" + cStatus);
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
}
