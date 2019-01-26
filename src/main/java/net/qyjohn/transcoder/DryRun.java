package net.qyjohn.transcoder;


import java.io.*;
import java.util.*;
import org.apache.log4j.Logger;

public class DryRun extends BasicWorker
{
	public DryRun(int id)
	{
		super(id);

	}

	@Override
	public void execute(String job)
	{		
		System.out.println(workerId + ": " + job);
	}

	public static void main(String args[])
	{
		try
		{
			int nProc = Runtime.getRuntime().availableProcessors();
			DryRun workers[] = new DryRun[nProc];
			for (int i=0; i<nProc; i++)
			{
				workers[i] = new DryRun(i);
			}
		} catch (Exception e)
		{
		}
	}
}
