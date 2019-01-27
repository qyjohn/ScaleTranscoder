package net.qyjohn.transcoder;

/**
 *
 * BasicWorker
 *
 * This BasicWorker provides a framework for multiple thread processing. It launches 
 * n threads where n equals to the number of vCPU cores available to the operating
 * system. Each thread polls from a queue for jobs to execute.
 *
 * The BasicWorker can be extended by overriding the execute() method to do different
 * type of work.
 *
 */


import java.io.*;
import java.util.*;
import com.rabbitmq.client.*;

public class BasicWorker
{
	public int workerId;
	public String mqHostname;
	public String mqJobQueue;
	public String region;
	public Connection connection;
	public Channel channel;

	ObjectStorageHandler storage;
	String bucketIn, bucketOut, action;
	public String workDir;
	long time1 = System.currentTimeMillis();
	// Test files
	String[] movies;

	public BasicWorker(int id)
	{
		workerId = id;

		try
		{
			// Getting runtime configuration from config.properties
			Properties prop = new Properties();
			InputStream input = new FileInputStream("config.properties");
			prop.load(input);
			mqHostname = prop.getProperty("mqHostname");
			mqJobQueue = prop.getProperty("mqJobQueue");
			region = prop.getProperty("region");
			workDir = prop.getProperty("workDir");
			movies  = prop.getProperty("movies").split(";");

			// Cloud-specific configurations
			storage = new S3Handler(region);
			bucketIn  = prop.getProperty("awsS3BucketIn");
			bucketOut = prop.getProperty("awsS3BucketOut");

			// RabbitMQ
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost(mqHostname);
			connection = factory.newConnection();
			channel = connection.createChannel();
			channel.queueDeclare(mqJobQueue, true, false, false, null);

			// Queue Consumer
			channel.basicQos(1);
			final Consumer consumer = new DefaultConsumer(channel) 
			{
				@Override
				public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException 
				{
					String message = new String(body, "UTF-8");
					execute(message);
					channel.basicAck(envelope.getDeliveryTag(), false);
				}
			};

			boolean autoAck = false;
			channel.basicConsume(mqJobQueue, autoAck, consumer);
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}


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
			int nProc = Runtime.getRuntime().availableProcessors();
			BasicWorker workers[] = new BasicWorker[nProc];
			for (int i=0; i<nProc; i++)
			{
				workers[i] = new BasicWorker(i);
			}
		} catch (Exception e)
		{
		}
	}
}
