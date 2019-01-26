package net.qyjohn.transcoder;

import java.io.*;
import java.net.*;

public class SpeedTest extends Thread
{
	String url;
	
	public SpeedTest(String url)
	{
		this.url = url;
	}

	public void run()
	{
		try
		{
                        byte[] buffer = new byte[1024*1024];
                        InputStream in = new URL(url).openStream();
                        int size = 0, length = 0;
                        boolean go = true;
                        while (size != -1)
                        {
                                size = in.read(buffer);
                        }
                        in.close();
		} catch (Exception e)
		{
		}
	}

	public static void main(String[] args)
	{
		try
		{
			String url = args[0];
			int repeat = Integer.parseInt(args[1]);

			byte[] buffer = new byte[1024*1024];

			// Warming up test
			long time0 = System.currentTimeMillis();
			InputStream in = new URL(url).openStream();
			int size = 0, length = 0;
			boolean go = true;
			while (size != -1)
			{
				size = in.read(buffer);
				length = length + size;
			}
			in.close();
			long time1 = System.currentTimeMillis();
			long t = time1 - time0;
			float speed = length * 1000 / t;
			System.out.println("Data Size: " + length);
			System.out.println("Time: " + t);
			System.out.println("Speed: " + speed + "Bytes per second");

			try
			{
				int nProc = Runtime.getRuntime().availableProcessors();
				for (int i=0; i<=repeat; i++)
				{
					// Do this N times
			                time0 = System.currentTimeMillis();
					SpeedTest workers[] = new SpeedTest[nProc];

					for (int j=0; j<nProc; j++)
					{
						workers[j] = new SpeedTest(url);
						workers[j].start();
					}
					for (int j=0; j<nProc; j++)
					{
						workers[j].join();
					}

					time1 = System.currentTimeMillis();
					t = time1 - time0;
					System.out.println(time1 + "\t" + time0 + "\t" + t);
					speed = (nProc * length) / t;
					speed = (1000 * speed) / (1024 * 1024);
				
		                	System.out.println("Speed: " + speed + "MBps per second");
				}
			} catch (Exception e1)
			{
				System.out.println(e1.getMessage());
				e1.printStackTrace();
			}
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
}
