package net.qyjohn.transcoder;


import java.io.*;
import java.sql.*;
import java.util.*;
import org.apache.log4j.Logger;

public class Transcoder extends BasicWorker
{
	ObjectStorageHandler storage;
	String bucketIn, bucketOut, action;
	public String workDir;
	long time1 = System.currentTimeMillis();
	// Test files
	String[] movies;


	public Transcoder(int id)
	{
		super(id);

		try
		{
			// Getting runtime configuration from config.properties
			Properties prop = new Properties();
			InputStream input = new FileInputStream("config.properties");
			prop.load(input);
			workDir = prop.getProperty("workDir");
			movies  = prop.getProperty("movies").split(";");

			// Cloud-specific configurations
			storage = new S3Handler();
			bucketIn  = prop.getProperty("awsS3BucketIn");
			bucketOut = prop.getProperty("awsS3BucketOut");
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
	
	@Override
	public void execute(String job)
	{		
		try
		{
			String[] arr = job.split(" ");
			action = arr[0];
			job = arr[1];

			if (action.equals("upload"))
			{
				String key = job+".mp4";
				int i = new Random().nextInt(movies.length);
				storage.upload(bucketIn, key, movies[i]);
			}
			else if (action.equals("delete_source"))
			{
				String key = job+".mp4";
				storage.delete(bucketIn, key);
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
			else if (action.equals("delete_transcode"))
			{
				storage.delete(bucketOut, job+".wmv");
			}
			else if (action.equals("watermark"))
			{
				// Download the source file
				String key = job + ".mp4";
				String outFile = workDir + "/" + job + ".mp4";
				storage.download(bucketIn, key, outFile);

				// Watermark and save as wmv, upload to object storage
				watermark(job);

				// Delete the source file
				File file = new File(outFile);
				file.delete();
			}
			else if (action.equals("delete_watermark"))
			{
				storage.delete(bucketOut, job+"_mark.wmv");
			}
			else if (action.equals("resize"))
			{
				// Download the source file
				String key = job + ".mp4";
				String outFile = workDir + "/" + job + ".mp4";
				storage.download(bucketIn, key, outFile);

				// Resize and save as wmv, upload to object storage
				resize(job);

				// Delete the source file
				File file = new File(outFile);
				file.delete();
			}
			else if (action.equals("delete_resize"))
			{
				storage.delete(bucketOut, job+"_320.wmv");
			}
			else if (action.equals("hls"))
			{
				// Download the source file
				String key = job + ".mp4";
				String outFile = workDir + "/" + job + ".mp4";
				storage.download(bucketIn, key, outFile);

				// Resize and save as wmv, upload to object storage
				hls(job);

				// Delete the source file
				File file = new File(outFile);
				file.delete();
			}
			else if (action.equals("delete_hls"))
			{
				String prefix = job;
				storage.deletePrefix(bucketOut, prefix);
			}

			long time2 = System.currentTimeMillis();
			int time = (int) ((time2 - time1) / 1000);
			System.out.println(workerId + ": " + job + " at " + time + " s");
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

	public void watermark(String job)
	{
		try
		{
			// The input file is already in the workDir
			String inFile  = workDir + "/" + job + ".mp4";
			String outFile = workDir + "/" + job + "_mark.wmv";

			// Run the command the transcode
			String cmd = "/usr/bin/ffmpeg -i " + inFile + " -i /home/ubuntu/bird.png -filter_complex overlay=10:10 " + outFile;
			Process proc = Runtime.getRuntime().exec(cmd);
			BufferedReader stdInput = new BufferedReader(new InputStreamReader(proc.getInputStream()));
			BufferedReader stdError = new BufferedReader(new InputStreamReader(proc.getErrorStream()));

			// Use two separate threads to handle output (stdout / stderr) from the command line
			new IoThread(stdInput).start();
			new IoThread(stdError).start();
			proc.waitFor();
			
			// Upload the output file then delete it from workDir
			String key = job + "_mark.wmv";
			storage.upload(bucketOut, key, outFile);
			File file = new File(outFile);
			file.delete();
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}


	public void resize(String job)
	{
		try
		{
			// The input file is already in the workDir
			String inFile  = workDir + "/" + job + ".mp4";
			String outFile = workDir + "/" + job + "_320.wmv";

			// Run the command the transcode
			String cmd = "/usr/bin/ffmpeg -i " + inFile + " -vf scale=320:-1 " + outFile;
			Process proc = Runtime.getRuntime().exec(cmd);
			BufferedReader stdInput = new BufferedReader(new InputStreamReader(proc.getInputStream()));
			BufferedReader stdError = new BufferedReader(new InputStreamReader(proc.getErrorStream()));

			// Use two separate threads to handle output (stdout / stderr) from the command line
			new IoThread(stdInput).start();
			new IoThread(stdError).start();
			proc.waitFor();
			
			// Upload the output file then delete it from workDir
			String key = job + "_320.wmv";
			storage.upload(bucketOut, key, outFile);
			File file = new File(outFile);
			file.delete();
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}

	public void hls(String job)
	{
		try
		{
			// The input file is already in the workDir
			String inFile  = workDir + "/" + job + ".mp4";
			// HLS contains many files, we need to create a temp folder to store those files
			String tempDir = workDir + "/" + job;
			Process p0 = Runtime.getRuntime().exec("/bin/mkdir -p " + tempDir);
			p0.waitFor();
			String outFile = tempDir + "/index.m3u8";

			// Run the command the transcode
			String cmd = "/usr/bin/ffmpeg -i " + inFile + " -strict -2 -f hls " + outFile;
			Process proc = Runtime.getRuntime().exec(cmd);
			BufferedReader stdInput = new BufferedReader(new InputStreamReader(proc.getInputStream()));
			BufferedReader stdError = new BufferedReader(new InputStreamReader(proc.getErrorStream()));

			// Use two separate threads to handle output (stdout / stderr) from the command line
			new IoThread(stdInput).start();
			new IoThread(stdError).start();
			proc.waitFor();
			
			// Upload all files in the temp folder 
			File folder = new File(tempDir);
			for (File entry : folder.listFiles()) 
			{
				if (!entry.isDirectory()) 
				{
					String filePath = entry.getPath();
					String fileName = entry.getName();
					String key = job + "/" + fileName;
					storage.upload(bucketOut, key, filePath);
				}
			}
			
			// Delete the temp folder
			Process p1 = Runtime.getRuntime().exec("/bin/rm -Rf " + tempDir);
			p1.waitFor();
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
/*			try
			{
				// Wait for networking and other stuff to be ready. 
				Thread.sleep(60000);
			} catch (Exception e)
			{
				System.out.println(e.getMessage());
				e.printStackTrace();				
			}
*/
			int nProc = Runtime.getRuntime().availableProcessors();
			Transcoder workers[] = new Transcoder[nProc];
			for (int i=0; i<nProc; i++)
			{
//				Input argument args[0] is the cloud service provider.
				workers[i] = new Transcoder(i);
			}
		} catch (Exception e)
		{
		}
	}
}
