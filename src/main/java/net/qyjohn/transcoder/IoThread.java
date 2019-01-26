package net.qyjohn.transcoder;

import java.io.*;

public class IoThread extends Thread
	{
		BufferedReader in;
		public IoThread(BufferedReader in)
		{
			this.in = in;
		}

		public void run()
		{
			try
			{
				String s;
				while ((s = in.readLine()) != null) 
				{
//					System.out.println(s);
				}
			} catch (Exception e)
			{
				System.out.println(e.getMessage());
				e.printStackTrace();
			}
		} 
	}

