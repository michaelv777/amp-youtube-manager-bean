/**
 * 
 */
package amp.source.youtube.base;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;

import javax.ejb.Timer;
import javax.enterprise.concurrent.ManagedThreadFactory;

import org.apache.axis.utils.StringUtils;
import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import amp.common.api.impl.ToolkitConstants;
import amp.common.api.impl.ToolkitDataProvider;
import amp.common.api.impl.ToolkitSpringConfig;
import amp.jpa.entities.ThreadConfiguration;
import amp.jpa.entities.Worker;
import amp.jpa.entities.WorkerConfiguration;
import amp.jpa.entities.WorkerThread;
import amp.jpaentities.mo.ThreadMO;
import amp.source.youtube.interfaces.SourceWorkerBeanLocal;
import amp.source.youtube.interfaces.SourceWorkerBeanRemote;

/**
 * @author MVEKSLER
 *
 */
public abstract class SourceManagerBean extends SourceManagerStorage 
										implements SourceWorkerBeanRemote, 
												   SourceWorkerBeanLocal
{
	private static final Logger cLogger = 
			LoggerFactory.getLogger(SourceManagerBean.class);
	
	//---class variables
	protected ManagedThreadFactory cThreadFactory = null;
	
	protected ThreadPoolExecutor cThreadPoolExecutor = null;

	protected ApplicationContext cApplicationContext = null;
	
	protected ToolkitDataProvider cToolkitDataProvider = null;

	protected Properties cSpringProps = null;
	
	protected Worker cWorker = 
			new Worker();
	
	protected List<WorkerConfiguration> cWorkerConfiguration = 
			new LinkedList<WorkerConfiguration>();
	
	protected List<ThreadConfiguration> cWorkerThreadsConfiguration = 
			new LinkedList<ThreadConfiguration>();
	
	protected HashMap<ThreadMO, HashMap<String, ThreadConfiguration>> cWorkerThreads = 
			new HashMap<ThreadMO, HashMap<String, ThreadConfiguration>>();
	
	protected HashMap<String, String> cSystemConfig = 
			new HashMap<String, String>();
	
	protected HashMap<String, String> cBeanConfig = 
			new HashMap<String, String>();
	
	protected long wkmThreadPoolTimeOut = Long.MAX_VALUE;
	
	protected long wkmKeepAliveThread = 300000;
	
	protected long wkmKeepAliveTimer = 300000;
	
	protected boolean lcRes = true;
	
	//---getters/setters
	public ManagedThreadFactory getcThreadFactory() {
		return cThreadFactory;
	}

	public Worker getcWorker() {
		return cWorker;
	}

	public void setcWorker(Worker cWorker) {
		this.cWorker = cWorker;
	}

	public List<ThreadConfiguration> getcWorkerThreadsConfiguration() {
		return cWorkerThreadsConfiguration;
	}

	public void setcWorkerThreadsConfiguration(List<ThreadConfiguration> cWorkerThreadsConfiguration) {
		this.cWorkerThreadsConfiguration = cWorkerThreadsConfiguration;
	}

	public ThreadPoolExecutor getcThreadPoolExecutor() {
		return cThreadPoolExecutor;
	}

	public void setcThreadPoolExecutor(ThreadPoolExecutor cThreadPoolExecutor) {
		this.cThreadPoolExecutor = cThreadPoolExecutor;
	}

	public long getWkmThreadPoolTimeOut() {
		return wkmThreadPoolTimeOut;
	}

	public void setWkmThreadPoolTimeOut(long wkmThreadPoolTimeOut) {
		this.wkmThreadPoolTimeOut = wkmThreadPoolTimeOut;
	}

	public long getWkmKeepAliveThread() {
		return wkmKeepAliveThread;
	}

	public void setWkmKeepAliveThread(long wkmKeepAliveThread) {
		this.wkmKeepAliveThread = wkmKeepAliveThread;
	}

	public long getWkmKeepAliveTimer() {
		return wkmKeepAliveTimer;
	}

	public void setWkmKeepAliveTimer(long wkmKeepAliveTimer) {
		this.wkmKeepAliveTimer = wkmKeepAliveTimer;
	}

	public void setcThreadFactory(ManagedThreadFactory cThreadFactory) {
		this.cThreadFactory = cThreadFactory;
	}
	
	public HashMap<String, String> getcSystemConfig() {
		return cSystemConfig;
	}

	public HashMap<String, String> getcBeanConfig() {
		return cBeanConfig;
	}

	public void setcBeanConfig(HashMap<String, String> cBeanConfig) {
		this.cBeanConfig = cBeanConfig;
	}

	public void setcSystemConfig(HashMap<String, String> cSystemConfig) {
		this.cSystemConfig = cSystemConfig;
	}

	

	public List<WorkerConfiguration> getcWorkerConfiguration() {
		return cWorkerConfiguration;
	}

	public void setcWorkerConfiguration(List<WorkerConfiguration> cWorkerConfiguration) {
		this.cWorkerConfiguration = cWorkerConfiguration;
	}

	public ToolkitDataProvider getcToolkitDataProvider() {
		return cToolkitDataProvider;
	}

	public void setcToolkitDataProvider(ToolkitDataProvider cToolkitDataProvider) {
		this.cToolkitDataProvider = cToolkitDataProvider;
	}

	public Properties getcSpringProps() {
		return cSpringProps;
	}

	public void setcSpringProps(Properties cSpringProps) {
		this.cSpringProps = cSpringProps;
	}
	
	public ApplicationContext getcApplicationContext() {
		return cApplicationContext;
	}

	public void setcApplicationContext(ApplicationContext cApplicationContext) {
		this.cApplicationContext = cApplicationContext;
	}

	public HashMap<ThreadMO, HashMap<String, ThreadConfiguration>> getcWorkerThreads() {
		return cWorkerThreads;
	}

	public void setcWorkerThreads(HashMap<ThreadMO, HashMap<String, ThreadConfiguration>> cWorkerThreads) {
		this.cWorkerThreads = cWorkerThreads;
	}

	public boolean isLcRes() {
		return lcRes;
	}

	public void setLcRes(boolean lcRes) {
		this.lcRes = lcRes;
	}
	
	/*-------------------------------------------------------------------*/
	protected boolean initClassVariables()
	{
		boolean cRes = true;
		
		String  cMethodName = "";
	
		try
    	{
    		StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement ste = stacktrace[1];
	        cMethodName = ste.getMethodName();
    		
	        this.cWorkerThreads = 
	        		new HashMap<ThreadMO, HashMap<String, ThreadConfiguration>>();
	        
	        this.cSystemConfig = new HashMap<String, String>();
	        
    		//-----------------
    		if ( cRes )
    		{
    			cRes = this.configureSpringExt();
    		}
    		//-----------------
    		if ( cRes )
    		{
    			this.cToolkitDataProvider = (ToolkitDataProvider)
    						this.cApplicationContext.getBean("toolkitDataProvider");
    			
    			if ( null == this.cToolkitDataProvider )
	    		{
    				cRes = false;
    				
	    			cLogger.error(cMethodName + "::cToolkitDataProvider is NULL!");
	    		}
    			else
    			{
    				cRes = this.cToolkitDataProvider.isLcRes();
    				
    				cLogger.info(cMethodName + "::cToolkitDataProvider status is " + cRes);
    			}
    		}	
    		//-----------------
    		if ( cRes )
    		{
    			List<Class<? extends Object>> clazzes = this.cToolkitDataProvider.
    					gettDatabase().getPersistanceClasses();
    			
    			this.cToolkitDataProvider.
    					gettDatabase().getHibernateSession(clazzes);
				
    			this.setLcRes(cRes = this.cToolkitDataProvider.
    					gettDatabase().isLcRes());
    		}
    		//-----------------
    		
    		return cRes;	 
    	}
		catch(  NoSuchBeanDefinitionException nbd )
		{
			cLogger.error(cMethodName + "::" + nbd.getMessage());
    		
    		this.setLcRes(cRes = false);
    		return cRes;
		}
		catch(  BeansException be )
		{
			cLogger.error(cMethodName + "::" + be.getMessage());
    		
    		this.setLcRes(cRes = false);
    		return cRes;
		}
    	catch( Exception e)
    	{
    		cLogger.error(cMethodName + "::" + e.getMessage());
    		
    		this.setLcRes(cRes = false);
    		return cRes;
    	}
	}
	
	/*-----------------------------------------------------------------------------------*/
	protected boolean configureSpringExt() 
	{
		boolean cRes = true;
		
		try 
		{
			this.cApplicationContext = 
					new AnnotationConfigApplicationContext(ToolkitSpringConfig.class);

			return cRes;
		} 
		catch (Exception e) 
		{
			cLogger.error("problem with configuration files!" + e.getMessage());
			
			this.setLcRes(cRes = false);
			
			return cRes;
		}
	}
	
	@SuppressWarnings("unchecked")
	protected boolean getWorker(String cWorkerName) 
	{
		String cMethodName = "";
		
		String sqlQuery = "";
		
		Session hbsSession = null;
		
		Transaction tx = null;
		
		boolean cRes = true;
		
		try
		{
			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement ste = stacktrace[1];
	        cMethodName = ste.getMethodName();
	    
	        if ( null == cWorkerName )
    		{
    			cLogger.error(cMethodName + "::(null == cWorkerName)");
    			
    			cRes = false;
    		}
	       
    		//------
    		if ( cRes )
    		{
	    		if ( null == this.cToolkitDataProvider )
	    		{
	    			cLogger.error(cMethodName + "::cToolkitDataProvider is NULL for the Method:" + cMethodName);
	    			
	    			cRes = false;
	    		}
    		}
    		//------
    		if ( cRes )
    		{
    			sqlQuery = this.cToolkitDataProvider.gettSQL().getSqlQueryByFunctionName(cMethodName);
    			
    			if ( null == sqlQuery || StringUtils.isEmpty(sqlQuery))
        		{
        			cLogger.error(cMethodName + "::sqlQuery is NULL for the Method:" + cMethodName);
        			
        			cRes = false;
        		}
    		}
    		//------
    		if ( cRes )
    		{
    			hbsSession = this.cToolkitDataProvider.gettDatabase().getHbsSessions().openSession();
    			
    			SQLQuery cQuery = hbsSession.createSQLQuery(sqlQuery);
    			
    			cQuery.addEntity(Worker.class);
    			
    			cQuery.setString("workerName", cWorkerName);
    			
    			tx = hbsSession.beginTransaction();
    			
    			List<Worker> cWorkers = (List<Worker>)cQuery.list();
    			
    			if ( cWorkers != null && cWorkers.size() >= 1 )
    			{
    				this.cWorker = cWorkers.get(0);
    			}
				
				if ( null == this.cWorker )
				{
					cLogger.error("M.V. Custom::" + cMethodName + "::cConfiguration  is NULL!");
					
					cRes = false;
				}
    		}
    		
    		if ( tx != null )
			{
				tx.commit();
			}
    		
    		return cRes;
		}
		catch( Exception e)
		{
			cLogger.error("M.V. Custom::" + cMethodName + "::" + e.getMessage());
			
			e.printStackTrace();
			
			return ( cRes = false );
		}
		finally
		{
			
			if ( hbsSession != null )
    		{
    			hbsSession.close();
    		}
		}
	}
	
	@SuppressWarnings("unchecked")
	protected boolean getWorkerConfig(String cWorkerName) 
	{
		String cMethodName = "";
		
		String sqlQuery = "";
		
		Session hbsSession = null;
		
		Transaction tx = null;
		
		boolean cRes = true;
		
		try
		{
			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement ste = stacktrace[1];
	        cMethodName = ste.getMethodName();
	    
	        if ( null == cWorkerName )
    		{
    			cLogger.error(cMethodName + "::(null == cWorkerName)");
    			
    			cRes = false;
    		}
	       
    		//------
    		if ( cRes )
    		{
	    		if ( null == this.cToolkitDataProvider )
	    		{
	    			cLogger.error(cMethodName + "::cToolkitDataProvider is NULL for the Method:" + cMethodName);
	    			
	    			cRes = false;
	    		}
    		}
    		//------
    		if ( cRes )
    		{
    			sqlQuery = this.cToolkitDataProvider.gettSQL().getSqlQueryByFunctionName(cMethodName);
    			
    			if ( null == sqlQuery || StringUtils.isEmpty(sqlQuery))
        		{
        			cLogger.error(cMethodName + "::sqlQuery is NULL for the Method:" + cMethodName);
        			
        			cRes = false;
        		}
    		}
    		//------
    		if ( cRes )
    		{
    			hbsSession = this.cToolkitDataProvider.gettDatabase().getHbsSessions().openSession();
    			
    			SQLQuery cQuery = hbsSession.createSQLQuery(sqlQuery);
    			
    			cQuery.addEntity(WorkerConfiguration.class);
    			
    			cQuery.setString("workerName", cWorkerName);
    			
    			tx = hbsSession.beginTransaction();
    			
				this.cWorkerConfiguration = (List<WorkerConfiguration>)cQuery.list();
				
				if ( null == this.cWorkerConfiguration )
				{
					cLogger.error("M.V. Custom::" + cMethodName + "::cConfiguration  is NULL!");
					
					cRes = false;
				}
    		}
    		
    		if ( tx != null )
			{
				tx.commit();
			}
    		
    		return cRes;
		}
		catch( Exception e)
		{
			cLogger.error("M.V. Custom::" + cMethodName + "::" + e.getMessage());
			
			e.printStackTrace();
			
			return ( cRes = false );
		}
		finally
		{
			
			if ( hbsSession != null )
    		{
    			hbsSession.close();
    		}
		}
	}
	
	protected boolean setWorkerThreadsConfiguration() 
	{
		String cMethodName = "";
		
		boolean cRes = true;
		
		try
		{
			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement ste = stacktrace[1];
	        cMethodName = ste.getMethodName();
	       
    		//------
    		if ( cRes )
    		{
				if ( this.getcWorker() != null )
				{
					if ( this.getcWorker().getWorkerThreads() != null )
					{
    					for ( WorkerThread cWorkerThread : this.getcWorker().getWorkerThreads() )
    					{
    						amp.jpa.entities.Thread cThread = cWorkerThread.getThread();
    						
    						if ( cThread != null )
    						{
    							Set<ThreadConfiguration> cThreadConfigurations = cThread.getThreadConfigurations();
    							
    							if ( cThreadConfigurations != null )
    							{
    								this.getcWorkerThreadsConfiguration().addAll(cThreadConfigurations);
    							}
    						}
    					}
					}
				}
    		}
    		
    		return cRes;
		}
		catch( Exception e)
		{
			cLogger.error("M.V. Custom::" + cMethodName + "::" + e.getMessage());
			
			e.printStackTrace();
			
			return ( cRes = false );
		}
		finally
		{
			
		}
	}
	
	
	protected boolean setWorkerConfiguration() 
	{
		String cMethodName = "";
		
		boolean cRes = true;
		
		try
		{
			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement ste = stacktrace[1];
	        cMethodName = ste.getMethodName();
	    
	        if ( null == this.getcWorker() )
    		{
    			cLogger.error(cMethodName + "::(null == cWorker)");
    			
    			cRes = false;
    		}
	       
    		
    		if ( cRes )
    		{
				this.getcWorkerConfiguration().addAll(
						this.getcWorker().getWorkerConfigurations());
				
				if ( null == this.getcWorkerConfiguration() )
				{
					cLogger.error("M.V. Custom::" + cMethodName + "::cConfiguration  is NULL!");
					
					cRes = false;
				}
    		}
    		return cRes;
		}
		catch( Exception e)
		{
			cLogger.error("M.V. Custom::" + cMethodName + "::" + e.getMessage());
			
			e.printStackTrace();
			
			return ( cRes = false );
		}
		finally
		{
			
		}
	}
	
	/**
	 * 
	 */
	protected boolean setWorkerThreads() 
	{
		String cMethodName = "";
		
		boolean cRes = true;
		
		try
		{
			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement ste = stacktrace[1];
	        cMethodName = ste.getMethodName();
	        
	        for( ThreadConfiguration cThreadConfiguration : this.cWorkerThreadsConfiguration)
	        {
        		String cConfigKey   = cThreadConfiguration.getConfigkey();
				String cConfigValue = cThreadConfiguration.getConfigvalue();
        		
				amp.jpa.entities.Thread cThread = cThreadConfiguration.getThread();
				
				ThreadMO cThreadMO = 
						new ThreadMO(cThread);
				
        		cLogger.info("M.V. Custom::"   + cMethodName +
        				      ",Source="       + cThreadConfiguration.getSource().getName() +
        				      ",Thread="       + cThreadConfiguration.getThread().getName() +
	      					  ",cConfigKey="   + cConfigKey   + 
	      					  ",cConfigValue=" + cConfigValue);
        		
        		if ( this.cWorkerThreads.containsKey(cThreadMO))
        		{
        			HashMap<String, ThreadConfiguration> cThreadConfig = 
        					this.cWorkerThreads.get(cThreadMO);
        			
        			cThreadConfig.put(cConfigKey, cThreadConfiguration);
        		}
        		else
        		{
        			HashMap<String, ThreadConfiguration> cThreadConfig = 
        					new HashMap<String, ThreadConfiguration>();
        			
        			cThreadConfig.put(cConfigKey, cThreadConfiguration);
        			
        			this.cWorkerThreads.put(cThreadMO, cThreadConfig);
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
	protected boolean setSystemProperties() 
	{
		String cMethodName = "";
		
		boolean cRes = true;
		
		try
		{
			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement ste = stacktrace[1];
	        cMethodName = ste.getMethodName();
	        
	        for( WorkerConfiguration cConfiguration : this.cWorkerConfiguration )
	        {
	        	if ( (cConfiguration.getSource().getName().
	        			equals(ToolkitConstants.AMP_RUNTIME_SOURCE)) &&
	        		  cConfiguration.getSource().getSourcetypeM().getName().
	        		  	equals(ToolkitConstants.AMP_RUNTIME_SOURCE))
	        	{
	        		String cConfigKey   = cConfiguration.getConfigkey();
	        		String cConfigValue = cConfiguration.getConfigvalue();
	        		
	        		this.cSystemConfig.put(cConfigKey, cConfigValue);
	        		
	        		if ( System.getProperty(cConfigKey) != null )
	        		{
	        			System.clearProperty(cConfigKey);
	        		}
	        		
	        		System.setProperty(cConfigKey, cConfigValue);
	        	}
	        }
	       
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
	 * 
	 */
	protected boolean setManagerProperties() 
	{
		String cMethodName = "";
		
		boolean cRes = true;
		
		try
		{
			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement ste = stacktrace[1];
	        cMethodName = ste.getMethodName();
	        
	        HashMap<String, Field> cFields = this.getInheritedFields(this.getClass()) ;
	        
	        for( WorkerConfiguration cConfiguration : this.cWorkerConfiguration )
	        {
	        	String cConfigKey   = cConfiguration.getConfigkey();
        		String cConfigValue = cConfiguration.getConfigvalue();
        		
	        	if ( (cConfiguration.getSource().getName().
	        			equals(ToolkitConstants.AMP_YOUTUBE_SOURCE)) )
	        	{
	        		this.cBeanConfig.put(cConfigKey, cConfigValue);
	        	
	        		if ( cFields.containsKey(cConfigKey) )
	        		{
	        			Field cField = cFields.get(cConfigKey);
						
	        			Type type = (Type) cField.getGenericType();
					  	
	        			if ( type.equals(String.class ))
	        			{
	        				cField.set(this, cConfigValue);
	        			}
	        			else if ( type.equals(boolean.class ))
	        			{
	        				boolean cBoolSet = Boolean.parseBoolean(cConfigValue);
	        				cField.setBoolean(this, cBoolSet);	
	        			}
	        			else if ( type.equals(int.class ))
	        			{
	        				int cIntSet = Integer.parseInt(cConfigValue);
	        				cField.setInt(this, cIntSet);	
	        			}
	        			else if ( type.equals(long.class ))
	        			{
	        				long cIntSet = Long.parseLong(cConfigValue);
	        				cField.setLong(this, cIntSet);	
	        			}
	        		}
	        		
		        	
	        	}
	        }
	        
	        /*
	        if ( this.cBeanConfig.containsKey(ToolkitConstants.AMP_KEEP_ALIVE_THREAD))
			{
				try
				{
					String cKeepAliveTimeStr = this.cBeanConfig.get(ToolkitConstants.AMP_KEEP_ALIVE_THREAD);
					
					this.cKeepAliveThread = Long.valueOf(cKeepAliveTimeStr);
				}
				catch(Exception e)
				{
					cLogger.error(cMethodName + "::Check Settings for:" + ToolkitConstants.AMP_KEEP_ALIVE_THREAD);
				}
			}
	        
	        if ( this.cBeanConfig.containsKey(ToolkitConstants.AMP_KEEP_ALIVE_TIMER))
			{
				try
				{
					String cKeepAliveTimerStr = this.cBeanConfig.get(ToolkitConstants.AMP_KEEP_ALIVE_TIMER);
					
					this.cKeepAliveTimer = Long.valueOf(cKeepAliveTimerStr);
				}
				catch(Exception e)
				{
					cLogger.error(cMethodName + "::Check Settings for:" + ToolkitConstants.AMP_KEEP_ALIVE_THREAD);
				}
			}
	        */
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
	 * @param timer
	 * @param cMethodName
	 */
	protected boolean printTimerInfo(Timer timer) 
	{
		boolean cRes = true;
		
		String cMethodName = "";
		
		try
		{
			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement ste = stacktrace[1];
	        cMethodName = ste.getMethodName();
	        
	        if ( timer == null )
	        {
	        	System.out.println("M.V. Custom::" + cMethodName + "::timer is null");
	        	
	        	return false;
	        }
	        
	        System.out.println("M.V. Custom::" + cMethodName + "::" + "Timer Service : "  + timer.getInfo());
			System.out.println("M.V. Custom::" + cMethodName + "::" + "Current Time : "   + new Date());
			System.out.println("M.V. Custom::" + cMethodName + "::" + "Next Timeout : "   + timer.getNextTimeout());
			System.out.println("M.V. Custom::" + cMethodName + "::" + "Time Remaining : " + timer.getTimeRemaining());
			System.out.println("____________________________________________");
			
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
}
