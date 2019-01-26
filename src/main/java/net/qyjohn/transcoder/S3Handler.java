package net.qyjohn.transcoder;

import java.io.*;
import com.amazonaws.*;
import com.amazonaws.auth.*;
import com.amazonaws.auth.profile.*;
import com.amazonaws.regions.*;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;

public class S3Handler implements ObjectStorageHandler
{
        public AmazonS3Client client;

	public S3Handler()
	{
		client = new AmazonS3Client();
                client.configureRegion(Regions.AP_SOUTHEAST_2);
	}

	public void upload(String bucket, String key, String fileFullPath)
	{
		try
		{
			File file = new File(fileFullPath);
			client.putObject(bucket, key, file);
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}

	public void download(String bucket, String key, String fileFullPath)
	{
		try
		{
			File file = new File(fileFullPath);
			GetObjectRequest request = new GetObjectRequest(bucket, key);
			client.getObject(request, file);
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
	
	public void delete(String bucket, String key)
	{
		try
		{
			client.deleteObject(bucket, key);
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}		
	}
	
	public void deletePrefix(String bucket, String prefix)
	{
		try
		{
			for (S3ObjectSummary file : client.listObjects(bucket, prefix).getObjectSummaries())
			{
				client.deleteObject(bucket, file.getKey());
			}
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}				
	}
	
}
