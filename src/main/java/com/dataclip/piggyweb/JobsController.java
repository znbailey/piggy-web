package com.dataclip.piggyweb;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.Lifecycle;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
public class JobsController implements Lifecycle {
	
	private static final Logger LOG = LoggerFactory.getLogger(JobsController.class);
	
	private final ExecutorService threadPool = Executors.newFixedThreadPool(3);
	
	@RequestMapping(value="/jobs", method=RequestMethod.PUT)
	public void newJob(@RequestParam String script) {
		
		final String tmpFile;
		try {
			tmpFile = writeScriptToTempFile(script);
		} catch ( IOException ioe ) {
			LOG.error("Unable to write submitted script to temp file: " + ioe.getMessage(), ioe);
			return;
		}
		
		threadPool.submit(new Runnable() {
			public void run() {
				LOG.info("Submitting script to pig server...");
				List<ExecJob> jobs = submitJobs(tmpFile);
		        LOG.info("Submitted batch of " + jobs.size() + " jobs. Waiting for completion...");
		        waitForCompletion(jobs);
		        LOG.info("All jobs complete. Check task tracker for more info.");
			}
		});
		LOG.info("Job submitted successfully.");
	}

	private String writeScriptToTempFile(String script) throws IOException {
		//write script to tmp file
		final String tempDir = System.getProperty("java.io.tmpdir");
		final String fileName = "job-script-" + System.currentTimeMillis() + ".pig";
		final String tmpFile;
		if ( tempDir.endsWith(File.separator) ) {
			tmpFile = tempDir + fileName;
		} else {
			tmpFile = tempDir + File.separator + fileName;
		}
		
		LOG.info("Writing script to temp file at " + tmpFile);
		
		FileWriter writer = null;
		try {
			writer = new FileWriter(tmpFile);
			writer.write(script);
		} finally {
			if ( writer != null ) {
				writer.close();
			}
		}
		
		LOG.info("Successfully wrote script to temp file at " + tmpFile);
		
		return tmpFile;
	}
	
	private List<ExecJob> submitJobs(final String tmpFile) {
		List<ExecJob> jobs = null;
		try {
			PigServer pigServer = new PigServer(ExecType.MAPREDUCE);
			pigServer.setBatchOn();
			pigServer.registerScript(tmpFile);
	        jobs = pigServer.executeBatch();
		} catch ( ExecException execException ) {
			LOG.error("Error connecting to hadoop and submitting job: " + execException.getMessage(), execException);
			return null;
		} catch ( IOException ioe ) {
			LOG.error("Error connecting to hadoop and submitting job: " + ioe.getMessage(), ioe);
			return null;
		}
		return jobs;
	}
	
	private void waitForCompletion(List<ExecJob> jobs) {
		if ( jobs == null ) {
			return;
		}
		
		boolean allComplete;
        do {
            allComplete = true;
            for ( ExecJob job : jobs ) {
            	try {
            		allComplete &= job.hasCompleted();
            	} catch ( ExecException ee ) {
            		LOG.warn("Unable to check job " + job + " for completion. Assuming it has completed, though it may not have.", ee);
            		allComplete &= true;
            	}
            }
            
            try {
            	Thread.sleep(1000);
            } catch ( InterruptedException ie ) {
            	return;
            }
            
        } while ( !allComplete );
	}
	
	public void start() {}
	
	public boolean isRunning() {
		return threadPool.isShutdown();
	}
	
	public void stop() {
		LOG.info("Shutting down thread pool...");
		threadPool.shutdown();
		try {
			threadPool.awaitTermination(1L, TimeUnit.HOURS);
		} catch ( InterruptedException ie ) {
			return;
		}
	}
	
}
