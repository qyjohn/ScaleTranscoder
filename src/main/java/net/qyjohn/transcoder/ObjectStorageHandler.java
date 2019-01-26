package net.qyjohn.transcoder;

public interface ObjectStorageHandler
{
	public void upload(String bucket, String key, String fileFullPath);
	public void download(String bucket, String key, String fileFullPath);
	public void delete(String bucket, String key);
	public void deletePrefix(String bucket, String prefix);
}
