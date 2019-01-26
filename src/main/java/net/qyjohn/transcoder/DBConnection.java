package net.qyjohn.transcoder;


import java.io.*;
import java.sql.*;
import java.util.*;
import org.apache.log4j.Logger;

public class DBConnection extends Thread
{
	Connection conn;
	int count = 0, last = 0, step = 0;
	int interval = 10000; // default 10 seconds

	public DBConnection(int interval)
	{
		this.interval = interval;
		
		try 
		{
			// Getting database properties from db.properties
			Properties prop = new Properties();
			InputStream input = new FileInputStream("config.properties");
			prop.load(input);
			String db_hostname = prop.getProperty("db_hostname");
			String db_username = prop.getProperty("db_username");
			String db_password = prop.getProperty("db_password");
			String db_database = prop.getProperty("db_database");

			// Load the MySQL JDBC driver
			Class.forName("com.mysql.jdbc.Driver");
			String jdbc_url = "jdbc:mysql://" + db_hostname + "/" + db_database + "?user=" + db_username + "&password=" + db_password;
			conn = DriverManager.getConnection(jdbc_url);
			conn.setAutoCommit(false);
			conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED); 
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}

	public synchronized String getJob()
	{
		String job = null;
		try
		{
			String sql = "SELECT * FROM jobs WHERE status=0 LIMIT 1 FOR UPDATE";
			PreparedStatement ps = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			ResultSet rs = ps.executeQuery();
			if(rs.next())
			{
				job = rs.getString("job");
				rs.updateInt("status", 1);
				rs.updateRow();
				conn.commit();
			}
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();				
		}
		return job;
	}				

	public synchronized void markJobAsCompleted(String job)
	{
		try
		{
			String sql = "UPDATE jobs SET status=2 WHERE job=?";
			PreparedStatement ps = conn.prepareStatement(sql);
			ps.setString(1, job);
			ps.executeUpdate();
			conn.commit();
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
				String sql = "SELECT COUNT(*) AS count FROM jobs WHERE status=2";
				Statement statement = conn.createStatement();
				ResultSet resultSet = statement.executeQuery(sql);
				if (resultSet.next())
				{
					count = resultSet.getInt("count");
					step  = count - last;
					last = count;
					System.out.println("Records Processed: " + step);
					sleep(interval);
				}
			} catch (Exception e)
			{
				System.out.println(e.getMessage());
				e.printStackTrace();				
			}
		}
	}	
	
	public static void main(String args[])
	{
		try
		{
			DBConnection conn = new DBConnection(10000);
			conn.start();
		} catch (Exception e)
		{
		}
	}			
}
