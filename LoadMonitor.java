/**
 *
 * A simple utility to record timestamp and system load.
 *
 */
 
import java.lang.management.*;
import javax.management.*;
import java.io.*;
import java.net.*;
import java.sql.*;
import java.util.*;

public class LoadMonitor extends Thread
{	
	String node_ip, test_id;
	Connection db_connection;
	Properties prop = new Properties();
	String nicRxPath, nicTxPath, diskPath, cpuPath;
	int cpuUser, cpuSystem, cpuIdle, cpuIoWait, cpuTotal;
	float cpuPercentUser, cpuPercentSystem, cpuPercentIdle, cpuPercentIoWait;
	int diskReadSectors, diskWriteSectors, deltaDiskReadBytes, deltaDiskWriteBytes;
	int nicRxBytes, nicTxBytes, deltaNicRxBytes, deltaNicTxBytes;
	
	public LoadMonitor(String nic, String disk)
	{
		// Path to various information sources
		nicRxPath = "/sys/class/net/" + nic + "/statistics/rx_bytes";
		nicTxPath = "/sys/class/net/" + nic + "/statistics/tx_bytes";
		diskPath  = "/sys/class/block/" + disk + "/stat"; 
		cpuPath   = "/proc/stat";
		
		// Initialize all the parameters
		getUsage();
	}
	
	public void getUsage()
	{
		try
		{
			// Get CPU usage
			String[] cpuInfo = readOneLine(cpuPath).split("\\s+");
			int newCpuUser   = Integer.parseInt(cpuInfo[1]);
			int newCpuSystem = Integer.parseInt(cpuInfo[3]);
			int newCpuIdle   = Integer.parseInt(cpuInfo[4]);
			int newCpuIoWait = Integer.parseInt(cpuInfo[5]);	// this is part of cpuIdle
			int newCpuTotal    = cpuUser + cpuSystem + cpuIdle;
			float deltaCpuTotal  = newCpuTotal  - cpuTotal;
			int deltaCpuUser   = newCpuUser   - cpuUser;
			int deltaCpuSystem = newCpuSystem - cpuSystem;
			int deltaCpuIdle   = newCpuIdle   - cpuIdle;
			int deltaCpuIoWait = newCpuIoWait - cpuIoWait;
			cpuTotal  = newCpuTotal;
			cpuUser   = newCpuUser;
			cpuSystem = newCpuSystem;
			cpuIdle   = newCpuIdle;
			cpuIoWait = newCpuIoWait;
			cpuPercentUser   = 100 * (deltaCpuUser   / deltaCpuTotal);
			cpuPercentSystem = 100 * (deltaCpuSystem / deltaCpuTotal);
			cpuPercentIdle   = 100 * (deltaCpuIdle   / deltaCpuTotal);
			cpuPercentIoWait = 100 * (deltaCpuIoWait / deltaCpuTotal);			

			// Get disk I/O
			String[] diskInfo = readOneLine(diskPath).trim().split("\\s+");
			int newDiskReadSectors = Integer.parseInt(diskInfo[2]);
			int newDiskWriteSectors = Integer.parseInt(diskInfo[6]);
			deltaDiskReadBytes    = 512 * (newDiskReadSectors - diskReadSectors);
			diskReadSectors  = newDiskReadSectors;
			deltaDiskWriteBytes   = 512 * (newDiskWriteSectors - diskWriteSectors);
			diskWriteSectors = newDiskWriteSectors;
			
			// Get network I/O
			int newNicRxBytes = Integer.parseInt(readOneLine(nicRxPath).trim());
			int newNicTxBytes = Integer.parseInt(readOneLine(nicTxPath).trim());
			deltaNicRxBytes = newNicRxBytes - nicRxBytes;
			nicRxBytes = newNicRxBytes;
			deltaNicTxBytes = newNicTxBytes - nicTxBytes;
			nicTxBytes = newNicTxBytes;
		} catch (Exception e)
		{
			
		}
	}
	
	public void print()
	{
		System.out.println(cpuPercentUser + "\t" + cpuPercentSystem + "\t" + cpuPercentIdle + "\t" + cpuPercentIoWait
			+ "\t" + deltaDiskReadBytes + "\t" + deltaDiskWriteBytes
			+ "\t" + deltaNicRxBytes + "\t" + deltaNicTxBytes);
	}
	
	public String readOneLine(String file)
	{
		try
		{
			// Read one line from file
			BufferedReader reader = new BufferedReader(new FileReader(file));
			String line = reader.readLine();
			reader.close();
			return line;
		} catch (Exception e)
		{
			return null;
		}		
	}
	

	
	public void run()
	{
		while (true)
		{
			try
			{
				getUsage();
				print();
				sleep(1000);
			} catch (Exception e)
			{
				System.out.println(e.getMessage());
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args)
	{
		LoadMonitor lm = new LoadMonitor(args[0], args[1]);
		lm.start();
	}
	
}
