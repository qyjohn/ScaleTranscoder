package net.qyjohn.transcoder;


import java.io.*;
import java.sql.*;
import java.util.*;
import org.apache.log4j.Logger;

public class OldTranscoder extends Thread
{
	DBConnection conn;
	public int workerId;
	ObjectStorageHandler storage;
	public String region, bucketIn, bucketOut, workDir, action;
	long time1 = System.currentTimeMillis();
	// Test files
	String[] movies;


	public OldTranscoder(int id, DBConnection conn, String action)
	{
		workerId = id;
		this.conn = conn;
		this.action = action;
		try
		{
			// Getting runtime configuration from config.properties
			Properties prop = new Properties();
			InputStream input = new FileInputStream("config.properties");
			prop.load(input);
			region = prop.getProperty("region");
			workDir = prop.getProperty("workDir");
			movies  = prop.getProperty("movies").split(";");

			// Cloud-specific configurations
			storage = new S3Handler(region);
			bucketIn  = prop.getProperty("awsS3BucketIn");
			bucketOut = prop.getProperty("awsS3BucketOut");
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}


	public void run()
	{
		while (true)
		{
			try
			{
				String job = conn.getJob();				
				if (job != null)
				{	
					execute(job);
					conn.markJobAsCompleted(job);
				}
				else
				{
					sleep(1000);
				}	
			} catch (Exception e)
			{
				System.out.println(e.getMessage());
				e.printStackTrace();				
			}
		}	
	}
	
	public void execute(String job)
	{		
		try
		{
			if (action.equals("upload"))
			{
				String key = job+".mp4";
				int i = new Random().nextInt(movies.length);
				storage.upload(bucketIn, key, movies[i]);
			}
			else if (action.equals("transcode"))
			{
				// Download the source file
				String key = job + ".mp4";
				String outFile = workDir + "/" + job + ".mp4";
 				storage.download(bucketIn, key, outFile);

				// Transcode to wmv and upload to object storage
				transcode(job, "wmv");

				// Delete the source file
				File file = new File(outFile);
				file.delete();
			}

			long time2 = System.currentTimeMillis();
			int time = (int) ((time2 - time1) / 1000);
//			System.out.println(workerId + ": " + job + " at " + time + " s");
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}

	public void transcode(String job, String format)
	{
		try
		{
			// The input file is already in the workDir
			String inFile  = workDir + "/" + job + ".mp4";
			String outFile = workDir + "/" + job + "." + format;

			// Run the command the transcode
			String cmd = "/usr/bin/ffmpeg -i " + inFile + " " + outFile;
			Process proc = Runtime.getRuntime().exec(cmd);
			BufferedReader stdInput = new BufferedReader(new InputStreamReader(proc.getInputStream()));
			BufferedReader stdError = new BufferedReader(new InputStreamReader(proc.getErrorStream()));

			// Use two separate threads to handle output (stdout / stderr) from the command line
			new IoThread(stdInput).start();
			new IoThread(stdError).start();
			proc.waitFor();
			
			// Upload the output file then delete it from workDir
			String key = job + "." + format;
			storage.upload(bucketOut, key, outFile);
			File file = new File(outFile);
			file.delete();
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}

	public static void main(String args[])
	{
		try
		{
			DBConnection conn = new DBConnection(10000);
			int nProc = Runtime.getRuntime().availableProcessors();
			OldTranscoder workers[] = new OldTranscoder[nProc];
			for (int i=0; i<nProc; i++)
			{
				workers[i] = new OldTranscoder(i, conn, args[0]);
				workers[i].start();
			}
		} catch (Exception e)
		{
		}
	}
}
