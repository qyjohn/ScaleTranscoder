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

public class CheckJobs
{
	public String mqHostname;
	public String mqJobQueue;
	public Connection connection;
	public Channel channel;

	final static Logger logger = Logger.getLogger(CheckJobs.class);

	public CheckJobs()
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
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}

	public void count(int repeat, int period)
	{
		try
		{
			for (int i=0; i<repeat; i++)
			{
				long count = channel.messageCount​(mqJobQueue);
				logger.info("Remaining jobs: " + count);
				Thread.sleep(period);
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
			int repeat = Integer.parseInt(args[0]);
			int period = Integer.parseInt(args[1]);			
			CheckJobs checker = new CheckJobs();
			checker.count(repeat, period);
			checker.shutdown();
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}		
	}
}
