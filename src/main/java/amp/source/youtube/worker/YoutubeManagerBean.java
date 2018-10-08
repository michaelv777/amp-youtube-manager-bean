package amp.source.youtube.worker;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import javax.ejb.LocalBean;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.ejb.Timeout;
import javax.ejb.Timer;
import javax.ejb.TimerConfig;
import javax.ejb.TimerService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import amp.common.api.impl.ToolkitConstants;
import amp.jpa.entities.ThreadConfiguration;
import amp.jpaentities.mo.ThreadMO;
import amp.jpaentities.mo.WorkerThreadMO;
import amp.source.youtube.base.SourceManagerBean;
import amp.source.youtube.impl.YoutubeWorker;

/**
 * Session Bean implementation class YoutubeManagerBean
 */
@Singleton(name="YoutubeManagerBean", mappedName = "YoutubeManagerBean")
@LocalBean
@Startup
public class YoutubeManagerBean extends SourceManagerBean 
{
	private static final Logger cLogger = 
			LoggerFactory.getLogger(YoutubeManagerBean.class);
	
	//---class variables
	@Resource
	protected TimerService cTimerService = null;
	protected Timer cIntervalTimer = null; 
	
	//---getters/setters

	public TimerService getcTimerService() {
		return cTimerService;
	}

	public void setcTimerService(TimerService cTimerService) {
		this.cTimerService = cTimerService;
	}

	public Timer getcIntervalTimer() {
		return cIntervalTimer;
	}

	public void setcIntervalTimer(Timer cIntervalTimer) {
		this.cIntervalTimer = cIntervalTimer;
	}
	
	//---class methods
    public YoutubeManagerBean() 
    {
		String cMethodName = "";
		
		try
		{
			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement ste = stacktrace[1];
	        cMethodName = ste.getMethodName();
		
		}
		catch( Exception e)
		{
			cLogger.error("M.V. Custom::" + cMethodName + "::" + e.getMessage());
			
			e.printStackTrace();
			
			this.setLcRes(false);
		}
    }
    
	@PostConstruct
	public void init()
	{
		boolean cRes = true;
		
		String cMethodName = "";
		
		try
		{
			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement ste = stacktrace[1];
	        cMethodName = ste.getMethodName();
	        
	        if ( cRes )
	        {
	        	cRes = super.initClassVariables();
	        }
	        if ( cRes )
	        {
	        	cRes = this.handleWorkersConfig();
	        }
	        if ( cRes )
	        {
	        	cRes = this.initThreadsPool();
	        }
	        if ( cRes )
	        {
	        	cRes = this.initWorkersThreads();
	        }

	        this.setLcRes(cRes);
		
		}
		catch( Exception e)
		{
			cLogger.error("M.V. Custom::" + cMethodName + "::" + e.getMessage());
			
			e.printStackTrace();
			
			this.setLcRes(cRes = false);
		}
	}

	public void initJUnit()
	{
		boolean cRes = true;
		
		String cMethodName = "";
		
		try
		{
			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement ste = stacktrace[1];
	        cMethodName = ste.getMethodName();
	        
	        if ( cRes )
	        {
	        	cRes = super.initClassVariables();
	        }
	        if ( cRes )
	        {
	        	cRes = this.handleWorkersConfig();
	        }
	        if ( cRes )
	        {
	        	cRes = this.initThreadsPoolJUnit();
	        }
	        if ( cRes )
	        {
	        	cRes = this.initWorkersThreadsJUnit();
	        }
	
	        this.setLcRes(cRes);
		
		}
		catch( Exception e)
		{
			cLogger.error("M.V. Custom::" + cMethodName + "::" + e.getMessage());
			
			e.printStackTrace();
			
			this.setLcRes(cRes = false);
		}
	}

	/**
	 * @param cRes
	 * @param cClassName
	 * @return
	 */
	protected boolean handleWorkersConfig() 
	{
		boolean cRes = true;
		
		String cMethodName = "";
		@SuppressWarnings("unused")
		String cClassName =  "";
		
		try
		{
			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement ste = stacktrace[1];
	        cMethodName = ste.getMethodName();
	        cClassName  = this.getClass().getSimpleName();
	        
	        String cSourceWorker = ToolkitConstants.AMP_YOUTUBE_SOURCE_WORKER;
	        
	        if ( cRes )
			{
				cRes = this.getWorker(cSourceWorker);
			}
	        if ( cRes )
			{
				cRes = this.setWorkerConfiguration();
			}
			if ( cRes )
			{
				cRes = this.setWorkerThreadsConfiguration();
			}
			if ( cRes )
			{
			    cRes = this.setWorkerThreads();
			}
			if ( cRes )
			{
			    cRes = this.setManagerProperties();
			}
			if ( cRes )
			{
			    cRes = this.setSystemProperties();
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

	protected boolean initThreadsPool()
	{
		boolean cRes = true;
		
		String  cMethodName = "";
	
		try
    	{
    		StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement ste = stacktrace[1];
	        cMethodName = ste.getMethodName();
    		
	        String cClassName = this.getClass().getSimpleName();
	        
	        if ( cRes )
	        {
		        //---init threads factory
		        if ( null == this.cThreadFactory )
		        {
		        	this.cThreadFactory = new ManagedThreadFactoryImpl();
		        }
	        }
	        
	        if ( cRes )
	        {
		        //---init monitoring timer
		        TimerConfig timerConfig = new TimerConfig();
				timerConfig.setPersistent(false);
				timerConfig.setInfo(cClassName);
				
				this.cIntervalTimer = this.cTimerService.createIntervalTimer(
						0, this.wkmKeepAliveTimer, timerConfig);
	        }
	        
	        if ( cRes )
	        {
				//---init Executor Service
				if ( this.cWorkerThreads != null && this.cWorkerThreads.size() > 0 )
				{
					int cThreadsSize = this.cWorkerThreads.size();
					
					this.cThreadPoolExecutor = new ThreadPoolExecutor(
							cThreadsSize, 
							cThreadsSize,    
							this.wkmKeepAliveThread, 
							TimeUnit.MILLISECONDS,
							new LinkedBlockingQueue<Runnable>(cThreadsSize), 
							this.cThreadFactory); 
				}
	        }
	        
    		return cRes;
    	}
    	catch( Exception e)
    	{
    		cLogger.error(cMethodName + "::" + e.getMessage());
    		
    		e.printStackTrace();
    		
    		this.setLcRes(cRes = false);
    		
    		return cRes;
    	}
	}
	
	protected boolean initThreadsPoolJUnit()
	{
		boolean cRes = true;
		
		String  cMethodName = "";
	
		try
    	{
    		StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement ste = stacktrace[1];
	        cMethodName = ste.getMethodName();
	        
	        if ( cRes )
	        {
		        //---init threads factory
		        if ( null == this.cThreadFactory )
		        {
		        	this.cThreadFactory = new ManagedThreadFactoryImpl();
		        }
	        }
	        
	        if ( cRes )
	        {
				//---init Executor Service
				if ( this.cWorkerThreads != null && this.cWorkerThreads.size() > 0 )
				{
					int cThreadsSize = this.cWorkerThreads.size();
					
					this.cThreadPoolExecutor = new ThreadPoolExecutor(
							cThreadsSize, 
							cThreadsSize,    
							this.wkmKeepAliveThread, 
							TimeUnit.MILLISECONDS,
							new LinkedBlockingQueue<Runnable>(cThreadsSize), 
							this.cThreadFactory); 
				}
	        }
	        
    		return cRes;
    	}
    	catch( Exception e)
    	{
    		cLogger.error(cMethodName + "::" + e.getMessage());
    		
    		e.printStackTrace();
    		
    		this.setLcRes(cRes = false);
    		
    		return cRes;
    	}
	}
	
	/**
	 * 
	 */
	protected boolean initWorkersThreads() 
	{
		String cMethodName = "";
		
		boolean cRes = true;
		
		try
		{
			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement ste = stacktrace[1];
	        cMethodName = ste.getMethodName();
	        
	        if ( this.cWorkerThreads != null && this.cThreadPoolExecutor != null)
	        {
	        	for( Map.Entry<ThreadMO, HashMap<String, ThreadConfiguration>> 
	        				cThreadConfigEntry : this.cWorkerThreads.entrySet() )
	        	{
	        		ThreadMO cThreadMO = cThreadConfigEntry.getKey();
	        		
	        		HashMap<String, ThreadConfiguration> cThreadConfig = 
	        				cThreadConfigEntry.getValue();
	        		
	        		WorkerThreadMO cWorkerThreadMO = 
	        				new WorkerThreadMO(cThreadMO.getcThread(), this.getcWorker());
	        		
	        		YoutubeWorker cWorker = new YoutubeWorker(
	        				cWorkerThreadMO, 
	        				cThreadConfig, 
	        				this.getcSystemConfig());
	        		
	        		this.cThreadPoolExecutor.execute(cWorker);
	        	}
	        	
	        	this.cThreadPoolExecutor.shutdown();
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

	protected boolean initWorkersThreadsJUnit() 
	{
		String cMethodName = "";
		
		boolean cRes = true;
		
		try
		{
			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement ste = stacktrace[1];
	        cMethodName = ste.getMethodName();
	        
	        if ( this.cWorkerThreads != null && this.cThreadPoolExecutor != null)
	        {
	        	for( Map.Entry<ThreadMO, HashMap<String, ThreadConfiguration>> 
				cThreadConfigEntry : this.cWorkerThreads.entrySet() )
				{
	        		
					ThreadMO cThreadMO = cThreadConfigEntry.getKey();
					
					HashMap<String, ThreadConfiguration> cThreadConfig = 
							cThreadConfigEntry.getValue();
					
					WorkerThreadMO cWorkerThreadMO = 
	        				new WorkerThreadMO(
	        						cThreadMO.getcThread(), this.getcWorker());
					
					YoutubeWorker cWorker = new YoutubeWorker(
							cWorkerThreadMO, 
	        				cThreadConfig, 
	        				this.getcSystemConfig());
	        		
					this.cThreadPoolExecutor.execute(cWorker);
				}
	        	
	        	this.cThreadPoolExecutor.shutdown();
	        	
	        	this.cThreadPoolExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
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
	
	@Timeout
	public void monitorWorkersThreads(Timer timer) 
	{
		String cMethodName = "";
		
		@SuppressWarnings("unused")
		boolean cRes = true;
		
		try
		{
			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement ste = stacktrace[1];
	        cMethodName = ste.getMethodName();
	        
	        this.printTimerInfo(timer);
	        
	        System.out.println(
                    String.format("[monitor] [%d/%d] Active: %d, Completed: %d, Task: %d, isShutdown: %s, isTerminated: %s",
                        this.cThreadPoolExecutor.getPoolSize(),
                        this.cThreadPoolExecutor.getCorePoolSize(),
                        this.cThreadPoolExecutor.getActiveCount(),
                        this.cThreadPoolExecutor.getCompletedTaskCount(),
                        this.cThreadPoolExecutor.getTaskCount(),
                        this.cThreadPoolExecutor.isShutdown(),
                        this.cThreadPoolExecutor.isTerminated()));
	        
	        //cRes = this.handleWorkersConfig();
		}
		catch( Exception e)
		{
			cLogger.error("M.V. Custom::" + cMethodName + "::" + e.getMessage());
			
			e.printStackTrace();
			
			this.setLcRes(cRes = false);
		}
		finally
		{
			try
			{
				this.setSourceItemStatus(
			        		cMethodName,
			        		ToolkitConstants.AMP_YOUTUBE_SOURCE, 
			        		ToolkitConstants.AMP_YOUTUBE_SOURCE,
			        		ToolkitConstants.AMP_YOUTUBE_SOURCE_WORKER,
			        		ToolkitConstants.AMP_DEFAULT_THREAD,
			        		cMethodName, 
						    (cRes = true) ? "Warning" : "Error", 
						    ToolkitConstants.OP_END_CYCLE);
			}
			catch( Exception e)
			{
				cLogger.error("M.V. Custom::" + cMethodName + "::" + e.getMessage());
				
				e.printStackTrace();
			}
		}
	}
	
	//---
	@PreDestroy
	public void releaseResources()
	{
		String cMethodName = "";
		
		try
		{
			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement ste = stacktrace[1];
	        cMethodName = ste.getMethodName();
	        
	        if ( this.cThreadPoolExecutor != null )
	        {
	        	this.cThreadPoolExecutor.shutdown();
	        }
	        
		}
		catch( Exception e)
		{
			cLogger.error("M.V. Custom::" + cMethodName + "::" + e.getMessage());
			
			e.printStackTrace();
		}
	}
}
