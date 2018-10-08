package amp.source.youtube.impl;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.net.URLEncoder;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.youtube.impl.RestYoutubeWorker;
import com.google.api.services.youtube.interfaces.RestYoutubeInterface;
import com.google.api.services.youtube.mo.SearchResultMO;
import com.google.api.services.youtube.model.SearchResult;

import amp.amazon.webservices.rest.Item;
import amp.amazon.webservices.rest.Items;
import amp.common.api.impl.ToolkitConstants;
import amp.data.handler.aws.DataHandlerAWS;
import amp.data.handler.base.DataHandlerI;
import amp.jpa.entities.Credential;
import amp.jpa.entities.Node;
import amp.jpa.entities.Source;
import amp.jpa.entities.ThreadConfiguration;
import amp.jpa.entities.ThreadScopeCredential;
import amp.jpa.entities.WorkerData;
import amp.jpa.entities.WorkerThread;
import amp.jpa.entities.WorkerThreadScope;
import amp.jpaentities.mo.CategoryMO;
import amp.jpaentities.mo.NodeMO;
import amp.jpaentities.mo.WorkerDataListMO;
import amp.jpaentities.mo.WorkerDataMO;
import amp.jpaentities.mo.WorkerThreadMO;
import amp.jpaentities.mo.WorkerThreadScopeMO;
import amp.source.youtube.base.ThreadWorker;
import text.processor.api.impl.TextProcessorDandelionImpl;
import text.processor.api.impl.TextProcessorWatsonImpl;
import text.processor.api.interfaces.TextProcessorInterface;
import text.translator.api.impl.TextTranslatorGoogleImpl;
import text.translator.api.interfaces.TextTranslatorInterface;

/**
 * Session Bean implementation class FacebookWorkerBean
 */
public class YoutubeWorker extends ThreadWorker implements Runnable
{
	private static final Logger cLogger = 
			LoggerFactory.getLogger(YoutubeWorker.class);
	
	//---class variables
	protected RestYoutubeInterface cRestYoutubeWorker = 
			new RestYoutubeWorker();
	
	protected boolean wkIsProcessComments     = false;
	protected boolean wkIsSendPostsAdvLinks   = false;
	protected boolean wkIsResendPostsAdvLinks = false;
	
	protected long wkInitialTimeout   = 300000;
	protected long wkIntervalDuration = 300000;
	
	protected long wkGetPostsMinutesAgo       = 43200;
	protected long wkResendPostsAdvLinksCycle = 21600;
	
	protected int  wkSendPostAdvLinksNum = 1;
	protected int  wkPageLimit = 10;
	
	//---getters/setters
	public boolean isWkIsProcessComments() {
		return wkIsProcessComments;
	}

	public void setWkIsProcessComments(boolean wkIsProcessComments) {
		this.wkIsProcessComments = wkIsProcessComments;
	}

	public boolean isWkIsSendPostsAdvLinks() {
		return wkIsSendPostsAdvLinks;
	}

	public void setWkIsSendPostsAdvLinks(boolean wkIsSendPostsAdvLinks) {
		this.wkIsSendPostsAdvLinks = wkIsSendPostsAdvLinks;
	}

	public boolean isWkIsResendPostsAdvLinks() {
		return wkIsResendPostsAdvLinks;
	}

	public void setWkIsResendPostsAdvLinks(boolean wkIsResendPostsAdvLinks) {
		this.wkIsResendPostsAdvLinks = wkIsResendPostsAdvLinks;
	}

	public long getWkInitialTimeout() {
		return wkInitialTimeout;
	}

	public void setWkInitialTimeout(long wkInitialTimeout) {
		this.wkInitialTimeout = wkInitialTimeout;
	}

	public long getWkIntervalDuration() {
		return wkIntervalDuration;
	}

	public void setWkIntervalDuration(long wkIntervalDuration) {
		this.wkIntervalDuration = wkIntervalDuration;
	}

	public long getWkGetPostsMinutesAgo() {
		return wkGetPostsMinutesAgo;
	}

	public void setWkGetPostsMinutesAgo(long wkGetPostsMinutesAgo) {
		this.wkGetPostsMinutesAgo = wkGetPostsMinutesAgo;
	}

	public long getWkResendPostsAdvLinksCycle() {
		return wkResendPostsAdvLinksCycle;
	}

	public void setWkResendPostsAdvLinksCycle(long wkResendPostsAdvLinksCycle) {
		this.wkResendPostsAdvLinksCycle = wkResendPostsAdvLinksCycle;
	}

	public int getWkSendPostAdvLinksNum() {
		return wkSendPostAdvLinksNum;
	}

	public void setWkSendPostAdvLinksNum(int wkSendPostAdvLinksNum) {
		this.wkSendPostAdvLinksNum = wkSendPostAdvLinksNum;
	}

	public int getWkPageLimit() {
		return wkPageLimit;
	}

	public void setWkPageLimit(int wkPageLimit) {
		this.wkPageLimit = wkPageLimit;
	}

	public RestYoutubeInterface getcRestYoutubeWorker() {
		return cRestYoutubeWorker;
	}

	public void setcRestYoutubeWorker(RestYoutubeInterface cRestYoutubeWorker) {
		this.cRestYoutubeWorker = cRestYoutubeWorker;
	}

	//---class methods
    public YoutubeWorker() 
    {
		String cMethodName = "";

		try
		{
			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement ste = stacktrace[1];
	        cMethodName = ste.getMethodName();
	        
	        super.initClassVariables();
	        
	        this.setLcRes(this.initClassVariables());
		}
		catch( Exception e)
		{
			cLogger.error("M.V. Custom::" + cMethodName + "::" + e.getMessage());
			
			e.printStackTrace();
			
			this.setLcRes(false);
		}
    }

    public YoutubeWorker(WorkerThreadMO cWorkerThreadMO, 
    					 HashMap<String, ThreadConfiguration> cThreadConfig) 
    {
    	String cMethodName = "";

		try
		{
			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement ste = stacktrace[1];
	        cMethodName = ste.getMethodName();
	        
	        super.initClassVariables();
	        
	        this.setcWorkerThreadMO(cWorkerThreadMO);
	        
	        this.setcThreadConfiguration(cThreadConfig);
	        
	        this.setThreadConfigurationMap(cThreadConfig);
	        
	        this.initClassVariables();
		}
		catch( Exception e)
		{
			cLogger.error("M.V. Custom::" + cMethodName + "::" + e.getMessage());
			
			e.printStackTrace();
			
			this.setLcRes(false);
		}
	}
    
    public YoutubeWorker(WorkerThreadMO cWorkerThreadMO, 
			  			  HashMap<String, ThreadConfiguration> cThreadConfig,
    				      HashMap<String, String> cSystemConfig) 
    {
    	String cMethodName = "";

		try
		{
			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement ste = stacktrace[1];
	        cMethodName = ste.getMethodName();
	        
	        super.initClassVariables();
	        
	        this.setcWorkerThreadMO(cWorkerThreadMO);
	        
	        this.setcThreadConfiguration(cThreadConfig);
	        
	        this.setcSystemConfiguration(cSystemConfig);
	        
	        this.setThreadConfigurationMap(cThreadConfig);
	        
	        this.initClassVariables();
		}
		catch( Exception e)
		{
			cLogger.error("M.V. Custom::" + cMethodName + "::" + e.getMessage());
			
			e.printStackTrace();
			
			this.setLcRes(false);
		}
	}

	@Override
	public void run() 
	{
		String cMethodName = "";
		
		@SuppressWarnings("unused")
		boolean cRes = true;
		
		try
		{
			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement ste = stacktrace[1];
	        cMethodName = ste.getMethodName();
	        
	        /*if ( this.wkInitialTimeout > 0 )
        	{
        		Thread.sleep(this.wkInitialTimeout);
        	}*/
	        
	        while ( this.wkIsRunThread )
	        {
	        	this.initNextCycle();
	        	
	        	this.processWorkerCycle();
		            
		        Thread.sleep(this.wkIntervalDuration);
	        }
		}
		catch( Exception e)
		{
			cLogger.error("M.V. Custom::" + cMethodName + "::" + e.getMessage());
			
			e.printStackTrace();
			
			this.setLcRes(cRes = false);
		}
	}

	@Override
	protected boolean initClassVariables()
	{
		String cMethodName = "";
		
		boolean cRes = true;
		
		try
		{
			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement ste = stacktrace[1];
	        
	        cMethodName = ste.getMethodName();
	     
	        this.setSystemConfiguration();
	     
	        this.setWorkerThreadConfiguration();
	        
	        this.cRestYoutubeWorker = new RestYoutubeWorker(
	        		this.cSystemConfiguration, 
	        		this.cThreadConfigurationMap);
	        
	        this.setLcRes(cRes);
		
	        return cRes;
		}
		catch( NumberFormatException e )
		{
			cLogger.error("M.V. Custom::" + cMethodName + "::" + e.getMessage());
			
			e.printStackTrace();
			
			this.setLcRes(cRes = false);
			
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

	/**
	 * 
	 */
	protected boolean initNextCycle() 
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

	protected boolean setSystemConfiguration() 
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
			System.out.println("M.V. Custom::" + cMethodName + "::" + e.getMessage());
			
			e.printStackTrace();
			
			this.setLcRes(cRes = false);
			
			return cRes;
		}
	}
	
	/**
	 * @throws IllegalAccessException
	 */
	protected boolean setWorkerThreadConfiguration()
	{
		String cMethodName = "";
		
		boolean cRes = true;
		
		try
		{
			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement ste = stacktrace[1];
	        cMethodName = ste.getMethodName();
	        
			Field[] cFields = this.getClass().getDeclaredFields();
			
			for( int jondex = 0; jondex < cFields.length; ++jondex )
			{
				Field cField = (Field)cFields[jondex];
				
				String cSourceConfigKey = cField.getName();
						 
				if ( this.cThreadConfiguration.containsKey(cSourceConfigKey))
				{
					String cSourceConfigValue = this.cThreadConfiguration.
							get(cSourceConfigKey).getConfigvalue();
					
					  Type type = (Type) cField.getGenericType();
					  	
					  if ( type.equals(String.class ))
					  {
						  cField.set(this, cSourceConfigValue);
					  }
					  else if ( type.equals(boolean.class ))
					  {
						  boolean cBoolSet = Boolean.parseBoolean(cSourceConfigValue);
						  cField.setBoolean(this, cBoolSet);	
					  }
					  else if ( type.equals(int.class ))
					  {
						  int cIntSet = Integer.parseInt(cSourceConfigValue);
						  cField.setInt(this, cIntSet);	
					  }
					  else if ( type.equals(long.class ))
					  {
						  long cIntSet = Long.parseLong(cSourceConfigValue);
						  cField.setLong(this, cIntSet);	
					  }
				}
			}
			//---set super class configuration
			cFields = getClass().getSuperclass().getDeclaredFields();
			
			for( int jondex = 0; jondex < cFields.length; ++jondex )
			{
				Field cField = (Field)cFields[jondex];
				
				String cSourceConfigKey = cField.getName();
						 
				if ( this.cThreadConfiguration.containsKey(cSourceConfigKey))
				{
					String cSourceConfigValue = this.cThreadConfiguration.
							get(cSourceConfigKey).getConfigvalue();
					
					  Type type = (Type) cField.getGenericType();
					  	
					  if ( type.equals(String.class ))
					  {
						  cField.set(this, cSourceConfigValue);
					  }
					  else if ( type.equals(boolean.class ))
					  {
						  boolean cBoolSet = Boolean.parseBoolean(cSourceConfigValue);
						  cField.setBoolean(this, cBoolSet);	
					  }
					  else if ( type.equals(int.class ))
					  {
						  int cIntSet = Integer.parseInt(cSourceConfigValue);
						  cField.setInt(this, cIntSet);	
					  }
					  else if ( type.equals(long.class ))
					  {
						  long cIntSet = Long.parseLong(cSourceConfigValue);
						  cField.setLong(this, cIntSet);	
					  }
				}
			}
			
			return cRes;
		}
		catch( IllegalAccessException e)
		{
			cLogger.error("M.V. Custom::" + cMethodName + "::" + e.getMessage());
			
			e.printStackTrace();
			
			this.setLcRes(cRes = false);
			
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

	/**
	 * 
	 */
	protected boolean processWorkerCycle() 
	{
		String cMethodName = "";
		
		boolean cRes = true;
		
		try
		{
			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement ste = stacktrace[1];
	        cMethodName = ste.getMethodName();
	        
	        Queue<NodeMO> queue = new LinkedList<NodeMO>();
	        /*
	         * 1. Use BFS to go through all the nodes
	         * 2. Search for the Youtube matching videos
	         * 3. Get Amazon node items
	         * 4. Send 1 item per video ( after increase number of the items to send ). 
	         */
	        
	        /*
	         * Init queue with the root nodes
	         */
	        amp.jpa.entities.Thread cThread = this.cWorkerThreadMO.getcThread();
	        
	        for ( WorkerThread cWorkerThread : cThread.getWorkerThreads() )
	        {
	        	Set<WorkerThreadScope>  cWorkerThreadScopes = cWorkerThread.getWorkerThreadScopes();
	        	
	        	for ( WorkerThreadScope cWorkerThreadScope : cWorkerThreadScopes )
	        	{
	        		if ( cWorkerThreadScope.getIsprimaryscope().longValue() == 1 )
	        		{
		        		NodeMO cRootNodeMO = new NodeMO(
		        				cWorkerThreadScope.getNode(), 
		        				cWorkerThreadScope.getNode());
		        	
		        		queue.add(cRootNodeMO);
	        		}
	        	}
	        }
	        
	        //---
	        while( !queue.isEmpty() )
			{
	        	NodeMO cNodeMO = queue.poll();
	        	
	        	this.processNodeData(cNodeMO);
	        	
	        	for( Node cChildNode : cNodeMO.getcNode().getNodes() )
				{
	        		NodeMO cChildNodeMO = new NodeMO(cChildNode);

	        		queue.add(cChildNodeMO);
				}
			}
			
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
	
	/**
	 * 
	 */
	protected boolean processNodeData(NodeMO cSearchNodeMO) 
	{
		String cMethodName = "";
		
		boolean cRes = true;
		
		try
		{
			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement ste = stacktrace[1];
	        cMethodName = ste.getMethodName();
	        
	        LinkedHashMap<WorkerThreadScopeMO, List<Items>> cSearchItems = 
					new LinkedHashMap<WorkerThreadScopeMO, List<Items>>();
	        
	        if ( cRes )
	        {
	        	/*
	        	 * Search by the Amazon Node name 
	        	 * 1:1 with the Youtube Search
	        	 */
	        	LinkedHashMap<Double, String> cTextKeywords = 
						new LinkedHashMap<Double, String>();
	        	
	        	cTextKeywords.put(new Double(100), cSearchNodeMO.getName());
	        
	        	/*
	        	 * Get amazon related products
	        	 * use all the configured credentials
	        	 */
	        	amp.jpa.entities.Thread cThread = this.cWorkerThreadMO.getcThread();
		        
		        for ( WorkerThread cWorkerThread : cThread.getWorkerThreads() )
		        {
		        	Set<WorkerThreadScope>  cWorkerThreadScopes = cWorkerThread.getWorkerThreadScopes();
		        	
		        	for ( WorkerThreadScope cWorkerThreadScope : cWorkerThreadScopes )
		        	{
		        		NodeMO cRootNodeMO = new NodeMO(cWorkerThreadScope.getNode());
		        		
		        		List<Items> cItems = this.searchAmazonForItems(
		        				cSearchNodeMO, cRootNodeMO, cWorkerThreadScope, cTextKeywords);
		        		
		        		WorkerThreadScopeMO cWorkerThreadScopeMO = 
		        				new WorkerThreadScopeMO(cWorkerThreadScope);
		        		
		        		if ( !cSearchItems.containsKey(cWorkerThreadScopeMO))
		        		{
		        			cSearchItems.put(cWorkerThreadScopeMO, cItems);
		        		}
		        	}
		        }
	        }
	        
	        if ( cRes )
	        {
				List<SearchResult> cPosts = this.cRestYoutubeWorker.getPosts(
						cSearchNodeMO.getcNode().getName(), 
						this.wkPageLimit, 
						this.wkGetPostsMinutesAgo);
				
				for( SearchResult cPost : cPosts )
				{
					boolean cPostRes = true; 
					
					SearchResultMO cPostMO = new SearchResultMO(cPost);
					
				    if ( this.isProcessYoutubePost(cPostMO, cSearchNodeMO) )
				    {
				    	cPostRes = this.processYoutubePost(
				    			cPostMO, cSearchNodeMO, cSearchItems);
				    }
					
				    String cPostStatus = (cPostRes == true ? 
			    			ToolkitConstants.AMP_STATUS_NORMAL : 
			    			ToolkitConstants.AMP_STATUS_WARNING);
				   
		            // Confirm that the result represents a video. Otherwise, the
		            // item will not contain a video ID.
			        if (cPostMO.getcKind().equals("youtube#video")) 
		            {
			        	String cDescription = cSearchNodeMO.getName()         + ":" + 
			        						  cSearchNodeMO.getBrowsenodeid() + ":" +
		                		 			  cPostMO.getcTitle()       + ":" + 
		                		 			  cPostMO.getcURL();
			        	
		                this.setItemOpStatus(
		                		 cPostMO.getcChannelId(),
		                		 cPostMO.getcItemId(), 
		                		 cDescription, 
								 cPostStatus, 
								 ToolkitConstants.OP_PROCESS_POST);
		            }
				}
	        }
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

	/**
	 * @param cMethodName
	 * @param cPost
	 */
	protected boolean processYoutubePost(
			SearchResultMO cPostMO,
			NodeMO cNodeMO,
			LinkedHashMap<WorkerThreadScopeMO, 
			List<Items>> cSearchItems) 
	{
		boolean cRes = true;
		
		String cMethodName = "";

		@SuppressWarnings("unused")
		String cTranslatedText = "";
	
		String cPostMessage = "";
		
		@SuppressWarnings("unused")
		LinkedHashMap<Double, String> cTextKeywords = 
				new LinkedHashMap<Double, String>();
		
		try
		{
			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement ste = stacktrace[1];
	        cMethodName = ste.getMethodName();
	        
	        if ( null == cPostMO )
	        {
	        	cRes = false;
	        }
	        //---get post message
	        if ( cRes )
	        {
	        	cPostMessage = cPostMO.getcTitle();
	        	if ( StringUtils.isBlank(cPostMessage))
	        	{
	        		cRes = false;
	        	}
	        }
	        //---translate post message
	        /*
	        if ( cRes )
	        {
				cTranslatedText = this.translateText(cPostMessage);
				if ( StringUtils.isBlank(cTranslatedText))
	        	{
	        		cRes = false;
	        	}
	        }
	        */	
	        /*//--get message keywords
	        if ( cRes )
	        {
	        	
	        	 * Search by the Video Title
	        	 
	        	//cTextKeywords.put(new Double(100), cPostMO.getcTitle());
	        	
	        	
	        	 * Search by the Amazon Node name 
	        	 * 1:1 with the Youtube Search
	        	 
	        	cTextKeywords.put(new Double(100), cNodeMO.getName());
	        }
	        //---get amazon related products
	        if ( cRes )
	        {
	        	cSearchItems.addAll(this.searchAmazonForItems(cNodeMO, cTextKeywords));
	        }*/
	        //---send post adv. links
	        if ( cRes )
	        {
	        	cRes = this.processAmazonItems(cNodeMO, cPostMO, cSearchItems);
	        }
	        
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

	/**
	 * @param cPost
	 * @param cSearchItems
	 */
	protected boolean processAmazonItems(
			NodeMO cNodeMO,
			SearchResultMO cPostMO, 
			LinkedHashMap<WorkerThreadScopeMO, List<Items>> cSearchItems) 
	{
		String cMethodName = "";
		
		boolean cRes = true;
		
		try
		{
			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement ste = stacktrace[1];
	        cMethodName = ste.getMethodName();
	        
	        this.sendPostAdvLinks(cNodeMO, cPostMO, cSearchItems);
			
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

	/**
	 * @param cPost
	 * @param cSearchItems
	 * @return
	 */
	protected boolean sendPostAdvLinks(
			NodeMO cNodeMO,
			SearchResultMO cPostMO, 
			LinkedHashMap<WorkerThreadScopeMO, 
			List<Items>> cScopeSearchItems) 
	{
		
		String cMethodName = "";
		
		boolean cRes = true;
		
		try
		{
			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement ste = stacktrace[1];
	        cMethodName = ste.getMethodName();
	        
	        Map<String, String> cRefferalLinks = 
	        		new LinkedHashMap<String, String>();
	        
	        String cChannelId = cPostMO.getcChannelId();
	        
			String cItemId = cPostMO.getcItemId();
			
			for( Map.Entry<WorkerThreadScopeMO, List<Items>> cSearchItemsEntry : 
				cScopeSearchItems.entrySet())
			{
				int sendPostLinkNum = 0;
				
				String cItemURLs  = "";
				
				WorkerThreadScopeMO cWorkerThreadScope = cSearchItemsEntry.getKey();
				
				List<Items> cSearchItems  = cSearchItemsEntry.getValue();
				
				for( Items cItemsM : cSearchItems )
				{
					List<Item> cItems = cItemsM.getItem();
					
					for( Item cItem : cItems )
					{
						if ( sendPostLinkNum < this.wkSendPostAdvLinksNum )
						{
							cItemURLs  += cItem.getDetailPageURL();
							cItemURLs  += System.lineSeparator();
							
							++sendPostLinkNum;
							
							System.out.println("M.V. Custom::Send Link" + cMethodName + "::" + cItem.getDetailPageURL());
							
							cLogger.debug("M.V. Custom::Send Link" + cMethodName + "::" + cItem.getDetailPageURL());
						}
					}
				}
				
				//---
				Source cSource = cWorkerThreadScope.getcWorkerThreadScope().getNode().getSource();
				String cFrom = cSource.getName();
				
				if ( StringUtils.isNotBlank(cItemURLs))
				{
					cRefferalLinks.put(cFrom, cItemURLs);
				}
			}
			
			
			StringBuilder cItemURLsMessage = new StringBuilder();
			String cUSLink = "";
			for( Map.Entry<String, String > cLinks : cRefferalLinks.entrySet())
			{
				if ( cLinks.getKey().endsWith("US"))
				{
					if ( !StringUtils.isEmpty(cLinks.getValue()) )
					{
						cUSLink = cLinks.getValue();
						//cUSLink += System.lineSeparator();
					}
				}
				else
				{
					if ( !StringUtils.isEmpty(cLinks.getValue()) )
					{
						//cItemURLsMessage.append(cLinks.getKey());
						cItemURLsMessage.append(cLinks.getValue());
						cItemURLsMessage.append(System.lineSeparator());
					}
				}
			}
			
			if ( StringUtils.isNotBlank(cUSLink))
			{
				cItemURLsMessage.insert(0, cUSLink);
			}
			
			boolean cSendLinkRes = 
						this.cRestYoutubeWorker.publishPostComment(
						cChannelId,		
						cItemId, 
						cItemURLsMessage.toString(), 
						cPostMO.getcMessage());

			String cDescription = 
					  cNodeMO.getName()         + ":" + 
					  cNodeMO.getBrowsenodeid() + ":" +
		 			  cPostMO.getcChannelId()   + ":" + 
		 			  cPostMO.getcURL();
			
			if ( cSendLinkRes )
			{
				this.setItemOpStatus(
						cPostMO.getcChannelId(),
						cPostMO.getcItemId(), 
						cDescription, 
			        	ToolkitConstants.AMP_STATUS_NOT_INPROCESS, 
						ToolkitConstants.OP_POST_LINK);
			}
			else
			{
				this.setItemOpStatus(
						cPostMO.getcChannelId(),
						cPostMO.getcItemId(), 
						cDescription, 
	        			ToolkitConstants.AMP_STATUS_CRITICAL, 
						ToolkitConstants.OP_POST_LINK);
			}
			
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

	/**
	 * @param cItemId
	 * @param isSendPostAdvLinks
	 * @return
	 */
	protected boolean isProcessYoutubePost(SearchResultMO cPostMO, NodeMO cNodeMO)
	{
		String cMethodName = "";
		
		boolean isProcessPost = false;
		
		try
		{
			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement ste = stacktrace[1];
	        cMethodName = ste.getMethodName();
		
			boolean isSendPostAdLinks = this.isSendPostAdLinks(cPostMO, cNodeMO);
			
			boolean isSendPostAdStatus = this.isSendPostAdStatus(cPostMO, cNodeMO);
			
			isProcessPost = (isSendPostAdLinks && isSendPostAdStatus );
			
			return isProcessPost;
		}
		catch( Exception e)
		{
			cLogger.error("M.V. Custom::" + cMethodName + "::" + e.getMessage());
			
			e.printStackTrace();
			
			this.setLcRes(isProcessPost = false);
			
			return isProcessPost;
		}
	}

	/**
	 * @return
	 */
	protected boolean isSendPostAdLinks(SearchResultMO cPostMO, NodeMO cNodeMO) 
	{
		String cMethodName = "";
		
		boolean isSendPostAdvLinks = false;
		
		try
		{
			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement ste = stacktrace[1];
	        cMethodName = ste.getMethodName();
			
	        if ( !this.wkIsSendPostsAdvLinks ) 
	        {
	        	return false;
	        }
	       
			String cItemId = cPostMO.getcItemId();
			
			WorkerDataListMO cWorkerDataListMO = this.getItemOpStatus(
							ToolkitConstants.AMP_AMAZON_SOURCE,
							ToolkitConstants.AMP_YOUTUBE_VIDEO_TARGET,
			        		//ToolkitConstants.AMP_FACEBOOK_SOURCE_WORKER,
			        		this.getcWorkerThreadMO().getcWorker().getName(),
			        		this.getcWorkerThreadMO().getcThread().getName(),
			    			cItemId, 
							ToolkitConstants.OP_POST_LINK);
		     
			if ( cWorkerDataListMO != null )
			{
				if ( cWorkerDataListMO.cWorkerData.size() == 0 )
				{
					isSendPostAdvLinks = true;
				}
				else if ( cWorkerDataListMO.cWorkerData.size() > 0 )
				{
					for ( WorkerDataMO cWorkerDataMO : cWorkerDataListMO.cWorkerData)
					{
						WorkerData cWorkerData = cWorkerDataMO.cWorkerData;
						
						//---if the same item and no itemId != ERROR
						if ( cWorkerData.getItemid().equals(cItemId) )
						{
							Date cCurrDate = Calendar.getInstance().getTime();
							Date cLinkDate = cWorkerData.getUpdatedate();
							
							long cTimeDiff = cCurrDate.getTime() - cLinkDate.getTime();
							
							long cMinDiff = TimeUnit.MILLISECONDS.toMinutes(cTimeDiff);
							
							if ( (cMinDiff >= this.wkResendPostsAdvLinksCycle) && (this.wkIsResendPostsAdvLinks) )
							{
								isSendPostAdvLinks = true;
							}
						}
					}
				}
			}
			
			return isSendPostAdvLinks;
		
		}
		catch( Exception e)
		{
			cLogger.error("M.V. Custom::" + cMethodName + "::" + e.getMessage());
			
			e.printStackTrace();
			
			this.setLcRes(isSendPostAdvLinks = false);
			
			return isSendPostAdvLinks;
		}
	}
	//---
	protected boolean isSendPostAdStatus(SearchResultMO cPostMO, NodeMO cNodeMO) 
	{
		String cMethodName = "";
		
		boolean isSendPostAdvLinks = false;
		
		try
		{
			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement ste = stacktrace[1];
	        cMethodName = ste.getMethodName();
	    
            String cItemId = cPostMO.getcItemId();
            
			WorkerDataListMO cWorkerDataListMO = this.getItemOpStatus(
							ToolkitConstants.AMP_AMAZON_SOURCE,
							ToolkitConstants.AMP_YOUTUBE_VIDEO_TARGET,
			        		this.getcWorkerThreadMO().getcWorker().getName(),
			        		this.getcWorkerThreadMO().getcThread().getName(),
			    			cItemId, 
							ToolkitConstants.OP_POST_LINK);
		     
			
			if ( cWorkerDataListMO != null )
			{
				if ( cWorkerDataListMO.cWorkerData.size() == 0 )
				{
					isSendPostAdvLinks = true;
				}
				else if ( cWorkerDataListMO.cWorkerData.size() > 0 )
				{
					for ( WorkerDataMO cWorkerDataMO : cWorkerDataListMO.cWorkerData)
					{
						WorkerData cWorkerData = cWorkerDataMO.cWorkerData;
						
						//---if the same item and no itemId != ERROR
						if ( cWorkerData.getItemid().equals(cItemId) )
						{
							String cPostStatus = cWorkerData.getStatusM().getName();
							
							if ( !cPostStatus.equals(ToolkitConstants.AMP_STATUS_NOT_INPROCESS) )
							{
								isSendPostAdvLinks = true;
							}
						}
					}
				}
			}
			
			return isSendPostAdvLinks;
		
		}
		catch( Exception e)
		{
			cLogger.error("M.V. Custom::" + cMethodName + "::" + e.getMessage());
			
			e.printStackTrace();
			
			this.setLcRes(isSendPostAdvLinks = false);
			
			return isSendPostAdvLinks;
		}
	}
	
	
	
	/**
	 * @param cSearchItemsM
	 */
	protected boolean printAmazonItems(List<Items> cSearchItemsM) 
	{
		String cMethodName = "";
	
		boolean cRes = true;
		
		try
		{
			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement ste = stacktrace[1];
	        cMethodName = ste.getMethodName();
	        
			for( Items cItemsM : cSearchItemsM )
			{
				List<Item> cItems = cItemsM.getItem();
				
				for( Item cItem : cItems )
				{
					System.out.println("M.V. Custom::" + cMethodName + "::" + cItem.getDetailPageURL());
					
					cLogger.debug("M.V. Custom::" + cMethodName + "::" + cItem.getDetailPageURL());
				}
			}
			
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
	
	
    protected boolean setThreadConfigurationMap(HashMap<String, ThreadConfiguration> cThreadConfig) 
	{
		String cMethodName = "";
	
		boolean cRes = true;
		
		try
		{
			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement ste = stacktrace[1];
	        cMethodName = ste.getMethodName();
	        
	        for( Map.Entry<String, ThreadConfiguration> cThreadConfigEntry : cThreadConfig.entrySet())
	        {
	        	ThreadConfiguration cThreadConfigValue = cThreadConfigEntry.getValue();
	        	
	        	String cConfigKey   = cThreadConfigValue.getConfigkey();
	        	String cConfigValue = cThreadConfigValue.getConfigvalue();
	        	
	        	this.cThreadConfigurationMap.put(cConfigKey, cConfigValue);
	        }
	        
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

	

	public String translateText(String text) 
	{
		String cMethodName = "";
	
		String cTranslatedText = "";
		
		try
		{
			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement ste = stacktrace[1];
	        cMethodName = ste.getMethodName();
	        	   
	        String textEncoded = URLEncoder.encode(text, "UTF-8");
        
	        TextTranslatorInterface cTextProcessor = 
	        		new TextTranslatorGoogleImpl(this.cSystemConfiguration);
	        //TextTranslatorInterface cTextProcessor = new TextTranslatorGoogleImpl();
	        		
	        String jsonResponse = cTextProcessor.translateTextByGet(textEncoded);
	        
	        System.out.println("M.V. Custom::" + cMethodName + "::" + jsonResponse);
	        
	        if ( !StringUtils.isEmpty(jsonResponse))
	        {
	        	cTranslatedText = cTextProcessor.getTranslatedData(jsonResponse);
	        	
	        	System.out.println("M.V. Custom::" + cMethodName + "::cKeywrodsText=" + cTranslatedText);
	        }
	        
	        return cTranslatedText;
		}
		catch( Exception e)
		{
			System.out.println("M.V. Custom::" + cMethodName + "::" + e.getMessage());
			
			e.printStackTrace();
			
			return text;
		}
	}
    
    public LinkedHashMap<Double, String> analyzeTextKeywordsWithWatson(String text) 
	{
		String cMethodName = "";
	
		LinkedHashMap<Double, String> cTextKeywords = 
				new LinkedHashMap<Double, String>();
		
		try
		{
			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement ste = stacktrace[1];
	        cMethodName = ste.getMethodName();
	        		
	        HashMap<String, String> params = new HashMap<String, String>();
	        params.put("text", text);
	        params.put("isEntitiesEmotion", "false");
	        params.put("isEntitiesSentiment", "false");
	        params.put("cEntitiesLimit", "1");
	        params.put("isKeywordsEmotion", "true");
	        params.put("isKeywordsSentiment", "true");
	        params.put("cKeywordsLimit", "2");
        
	        TextProcessorInterface cTextProcessor = 
	        		new TextProcessorWatsonImpl(this.cSystemConfiguration, 
	        								    this.cThreadConfigurationMap);
        	
	        String jsonResponse = cTextProcessor.extractDataFromTextByPost(params);
	        
	        System.out.println("M.V. Custom::" + cMethodName + "::" + jsonResponse);
	        
	        if ( !StringUtils.isEmpty(jsonResponse))
	        {
	        	cTextKeywords = cTextProcessor.getKeywordsFromJSONData(jsonResponse, 1, true);
	        	
	        	System.out.println("M.V. Custom::" + cMethodName + "::cKeywrodsText=" + cTextKeywords);
	        }
	        
	        return cTextKeywords;
		}
		catch( Exception e)
		{
			System.out.println("M.V. Custom::" + cMethodName + "::" + e.getMessage());
			
			e.printStackTrace();
			
			return new LinkedHashMap<Double, String>();
		}
	}
    
    public String analyzeTextKeywordsWithDandelion(String text) 
	{
		String cMethodName = "";
	
		String cTextKeywords = "";
		
		try
		{
			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement ste = stacktrace[1];
	        cMethodName = ste.getMethodName();
	        		
	        HashMap<String, String> params = new HashMap<String, String>();
	        params.put("text", text);
	        params.put("min_confidence", "0.5");
	        params.put("social.hashtag", "true");
	        params.put("social.mention", "true");
	        params.put("include", "types, categories");
        
	        TextProcessorInterface cTextProcessor = 
	        		new TextProcessorDandelionImpl(this.cSystemConfiguration);
        	
	        String jsonResponse = cTextProcessor.extractDataFromTextByGet(params);
	        
	        System.out.println("M.V. Custom::" + cMethodName + "::" + jsonResponse);
	        
	        if ( !StringUtils.isEmpty(jsonResponse))
	        {
	        	cTextKeywords = cTextProcessor.getKeywordsFromJSONData(jsonResponse);
	        	
	        	System.out.println("M.V. Custom::" + cMethodName + "::cKeywrodsText=" + cTextKeywords);
	        }
	        
	        return cTextKeywords;
		}
		catch( Exception e)
		{
			System.out.println("M.V. Custom::" + cMethodName + "::" + e.getMessage());
			
			e.printStackTrace();
			
			return "";
		}
	}
    
    public List<Items> searchAmazonForItems(
    		NodeMO cSearchNodeMO,
    		NodeMO cRootNodeMO,
    		WorkerThreadScope cWorkerThreadScope,
    		LinkedHashMap<Double, String> cTextKeywords)
 	{
 		String cMethodName = "";

 		List<Items> cSearchItems = new LinkedList<Items>();
 		
 		@SuppressWarnings("unused")
		boolean cRes = true;

 		try 
 		{
 			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
 			StackTraceElement ste = stacktrace[1];
 			cMethodName = ste.getMethodName();
 			
 			if ( cTextKeywords == null )
 			{
 				System.out.println("M.V. Custom::" + cMethodName + "::cTextKeywords is null!");
 				
 				return cSearchItems;
 			}
 			//---
 			if ( (cTextKeywords != null) && (cTextKeywords.size() >= 1) )
        	{
        		for( Map.Entry<Double, String> cTextKeywordsEntry : cTextKeywords.entrySet())
        		{
        			String cTextKeyword = cTextKeywordsEntry.getValue();
        			
        			Source cSource = cRootNodeMO.getcNode().getSource();
        			
        			//Source cSource = new Source();
        			//cSource.setName(ToolkitConstants.AMP_AMAZON_SOURCE);
        			
        			CategoryMO cCategory = new CategoryMO(cRootNodeMO.getName());
        			cCategory.getcCategory().setName(cRootNodeMO.getName());
        			cCategory.getcCategory().setSource(cSource);

        			/*
        			 * Set Root Category Name for the Search Index
        			 */
        			cCategory.getcCategory().setSearchindex(cRootNodeMO.getName());
        			
        			/*
        			 * Set Browse Node ID - required if the root category is not ALL
        			 */
        			cCategory.getcCategory().setRootbrowsenode(cSearchNodeMO.getBrowsenodeid());
        			
        			for ( ThreadScopeCredential cCredentials : cWorkerThreadScope.getThreadScopeCredentials())
        			{
        				String cSortValue = !StringUtils.isEmpty(cWorkerThreadScope.getNodesortvalue()) ?
        						cWorkerThreadScope.getNodesortvalue() : "";
        				
        				List<Items> cAmazonItems = this.getAmazonItems(
        						cCategory, cCredentials, cTextKeyword, cSortValue, 1);
        				
        				 cSearchItems.addAll(cAmazonItems);
        			}
        		}
        	}
 			
 			this.printAmazonItems(cSearchItems);
 			
 			return cSearchItems;
 		} 
 		catch (Exception e) 
 		{
 			this.setLcRes(cRes = false);
 			
 			System.out.println("M.V. Custom::" + cMethodName + "::" + e.getMessage());
 			
 			return new LinkedList<Items>();
 		}
 	}
    
    @SuppressWarnings("unchecked")
	public List<Items> getAmazonItems(CategoryMO cCategory,
									  ThreadScopeCredential cCredentials,
    								  String cKeywords,	
    								  String cSortValue,	
    								  long cItemPage)
	{
		boolean cRes = true;
		
		String  cMethodName = "";
		
		List<Items> cSearchItems = new LinkedList<Items>();
		
		try
		{
			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement ste = stacktrace[1];
	        cMethodName = ste.getMethodName();
	
	        //-----------------------------------------
	        Map<String, Object> cMethodParams = new TreeMap<String, Object>();
	        Map<String, Object> cMethodResults = new TreeMap<String, Object>();
	        
	        Map<String, String> params = new HashMap<String, String>();
	        
	        if ( null == cCategory.getName() )
	        {
	        	String cMesage = cMethodName + "::cSerachIndex is null!";
	        	
	        	System.out.println(cMesage);
	        	
	        	this.setLcRes(cRes = false);
	        }
	        
	        if ( null == cCategory.getRootbrowsenode() )
	        {
	        	String cMesage = cMethodName + "::cBrowseNode is null!";
	        	
	        	System.out.println(cMesage);
	        	
	        	this.setLcRes(cRes = false);
	        }
	        //-----------------------------------------
	        if ( cRes )
	        {
	        	String cSerachIndex = cCategory.getSearchIndex();
	        	String cBrowseNode = cCategory.getRootbrowsenode();
	        	
	        	if ( !cSerachIndex.equalsIgnoreCase("all"))
	        	{
	        		params.put("BrowseNode", cBrowseNode);
	        	}
	        	params.put("SearchIndex", cSerachIndex);
	 	        params.put("ResponseGroup", "Images,ItemAttributes,ItemIds,Offers,SalesRank");
	 	        
	 	        if ( StringUtils.isNotBlank(cSortValue))
	 	        {
	 	        	params.put("Sort", cSortValue);
	 	        }

	 	        params.put("Keywords", cKeywords);
	 	        params.put("Availability", "Available");
	 	        params.put("ItemPage", String.valueOf(cItemPage));
	 	        params.put("Condition", "All");
	 	        
	 	        /*
	 	         * add amazon server parameter 
	 	         */
	 	       params.put("Endpoint", cCredentials.getCredential().getAwsEndpoint());
	 	        /*
	 	         * add credentials parameters
	 	         */
	 	        Credential cCredential = cCredentials.getCredential();
	 	        
	 	        if ( cCredential != null )
	 	        {
		 	        params.put("AssociateTag", cCredential.getAwsAssociateTag());
		 	        params.put("AWSAccessKeyId", cCredential.getAwsAccessKeyId());
		 	        params.put("AWSAccessSecretKey", cCredential.getAwsSecretKey());
	 	        }
	 	        cMethodParams.put("p1", params);
	 	        
	 	        DataHandlerI cDataHandlerAWS = new DataHandlerAWS(this.cSystemConfiguration,
	 	        												  this.cThreadConfigurationMap);
	 	        
	 	        cRes = cDataHandlerAWS.handleItemSearchList(cMethodParams, cMethodResults);
	        }
	        //---
	        if ( cRes )
	        {
	        	cSearchItems =  (List<Items>)cMethodResults.get("r1");
	        	
	        	if ( null == cSearchItems )
	        	{
	        		String cMesage = cMethodName + "::Categories Nodes list is null!";
	        	
	        		System.out.println(cMesage);
	        		
	        		this.setLcRes(cRes = false);
	        	}
	        }
	       
	        this.setLcRes(cRes = true); 
	        
	        return cSearchItems;
		}
		catch( Exception e)
		{
			System.out.println("M.V. Custom::" + cMethodName + "::" + e.getMessage());
			
			this.setLcRes(cRes = false); 
			
			return new LinkedList<Items>();
		}
	}

}
