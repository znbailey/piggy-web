package com.dataclip.piggyweb;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.DFSClient;
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
import org.springframework.web.client.RestTemplate;

@Controller
public class JobsController implements Lifecycle {
	
	private static final Logger LOG = LoggerFactory.getLogger(JobsController.class);
	
	private final ExecutorService threadPool = Executors.newFixedThreadPool(3);
	
	@RequestMapping(value="/results")
	public String jobResults(HttpServletResponse response, @RequestParam String path) {
		LOG.info("Got request for results in path: " + path);
		response.setCharacterEncoding("utf-8");
		response.setContentType("text/plain");
		try {
			DFSClient dfsClient = new DFSClient(new Configuration(true));
			
			if ( !dfsClient.exists(path) ) {
				LOG.error("Results path " + path + " does not exist. Returning 404.");
				response.setStatus(404);
				return null;
			}
			
			OutputStream out = response.getOutputStream();
			
			FileStatus status = dfsClient.getFileInfo(path);
			if ( status.isDir() ) {
				FileStatus[] files = dfsClient.listPaths(path);
				LOG.info("Results path " + path + " is directory. Contains " + files.length + " children.");
				long totalSize = 0L;
				for ( FileStatus file : files ) {					
					if ( !file.isDir() ) {
						totalSize += file.getLen();
					}
				}
				response.setContentLength((int)totalSize);
				for ( FileStatus file : files ) {					
					if ( !file.isDir() ) {
						LOG.info("Streaming individual file " + file.getPath().toString() + " out of results path " + path);
						streamFile(file.getPath().toString(), dfsClient, out);
					}
				}
			} else {
				LOG.info("Results path " + path + " is single file. Streaming...");
				response.setContentLength((int)status.getLen());
				streamFile(path, dfsClient, out);
			}			
			response.setStatus(200);
		} catch (IOException e) {
			response.setStatus(500);
			LOG.error("Unable to read from HDFS: " + e.getMessage(), e);
		}
		LOG.info("Done serving results at path " + path);
		return null;
	}

	private void streamFile(String path, DFSClient dfsClient, OutputStream out)
			throws IOException {
		
		BufferedInputStream in = new BufferedInputStream(dfsClient.open(path));
		byte[] buf = new byte[4096];
		while ( in.read(buf) > 0 ) {
			out.write(buf);
		}
	}
	
	@RequestMapping(value="/jobs", method=RequestMethod.PUT)
	public void newJob(@RequestParam String script, @RequestParam final String callbackUrl) {
		
		final String tmpFile;
		try {
			tmpFile = writeScriptToTempFile(script);
		} catch ( IOException ioe ) {
			LOG.error("Unable to write submitted script to temp file: " + ioe.getMessage(), ioe);
			return;
		}
		
		threadPool.submit(new Runnable() {
			public void run() {
				try {
					LOG.info("Submitting script to pig server...");
					List<ExecJob> jobs = submitJobs(tmpFile);
			        
					LOG.info("Submitted batch of " + jobs.size() + " jobs. Waiting for completion...");
			        waitForCompletion(jobs);
			        
			        LOG.info("All jobs complete. Check task tracker for more info.");
			        RestTemplate restClient = new RestTemplate();
			        
			        //ping the app and let it know the job is done
			        restClient.getForObject(callbackUrl, Response.class);
				} catch ( Throwable t ) {
					LOG.error("Error running pig script: " + t.getMessage(), t);
				}
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
	
	static class Response {
		boolean success;
		public boolean isSuccess() { return success; }
		public void setSuccess(boolean success) { this.success = success; }				
	}
	
}
