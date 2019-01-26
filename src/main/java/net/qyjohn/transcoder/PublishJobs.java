package net.qyjohn.transcoder;

/**
 *
 * PublishJobs
 *
 * This job publishes a set of messages to a queue, which serve as the jobs
 * to be processed by the workers. The program works with an input file, 
 * each line in the input file represents a job.
 *
 */

import java.io.*;
import java.util.*;
import com.rabbitmq.client.*;
import org.apache.log4j.Logger;

public class PublishJobs
{
	public String mqHostname;
	public String mqJobQueue;
	public Connection connection;
	public Channel channel;
	final static Logger logger = Logger.getLogger(PublishJobs.class);

	public PublishJobs()
	{
		try
		{
			// Getting runtime configuration from config.properties
			Properties prop = new Properties();
			InputStream input = new FileInputStream("config.properties");
			prop.load(input);
			mqHostname = prop.getProperty("mqHostname");
			mqJobQueue = prop.getProperty("mqJobQueue");

			// RabbitMQ
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost(mqHostname);
			connection = factory.newConnection();
			channel = connection.createChannel();
			channel.queueDeclare(mqJobQueue, true, false, false, null);
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}

	public void send(String message)
	{
		try
		{
			channel.basicPublish("", mqJobQueue,
				MessageProperties.PERSISTENT_TEXT_PLAIN,
				message.getBytes("UTF-8"));

		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}

	public void count(int period)
	{
		try
		{
			long count = 1L, last = 1L, step = 1L;
			while (count != 0)
			{
				count = channel.messageCount​(mqJobQueue);
				step = last - count;
				last = count;
				//logger.info("Processed Jobs: " + step);
				System.out.println(step);
				if (count != 0)
				{
					Thread.sleep(period);
				}
			}
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}		
	}

	public void shutdown()
	{
		try
		{
			channel.close();
			connection.close();
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}

	public static void main(String[] args)
	{
		try
		{
			PublishJobs publisher = new PublishJobs();
			File input = new File(args[0]);
			String action = args[1];
			int monitorPeriod = Integer.parseInt(args[2]);
			Scanner sc = new Scanner(new FileInputStream(input));
			String line;
			publisher.logger.info("Submitting jobs.");
			
			int count = 0;
			long start = System.currentTimeMillis() / 1000;
			while (sc.hasNextLine())
			{
				line = sc.nextLine().trim();
				if (line.length() != 0)
				{
					publisher.send(action + " " + line);
					count++;
				}
			}
			publisher.logger.info("Jobs submitted.");
			publisher.count(monitorPeriod);
			publisher.shutdown();
			long end = System.currentTimeMillis() / 1000;
			
			long cost = end - start;
			float rate = (float) count / (float) cost;
			System.out.println("\nTotal   Execution Time: " + cost);
			System.out.println("\nAverage Execution Rate: " + rate + "\n");

			
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}		
	}
}
