package com.analysis.format;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PeopleOutputFormat<K,V> extends FileOutputFormat<K, V>{

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new SortRecordWriter<K,V>();
	}
	
	public static class SortRecordWriter<K,V> extends RecordWriter<K, V>{
		
		
			// TODO Auto-generated constructor stub
			
			
		
		@Override
		public void write(K key, V value) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//System.out.println(key.toString()+":"+value.toString());
			Configuration conf = HBaseConfiguration.create();
			//conf.set("hbase.zookeeper.quorum", "10.105.245.143,10.105.245.144,10.105.245.147");
			conf.set("hbase.zookeeper.quorum", "localhost");
			String time = key.toString();
			int  sum = Integer.parseInt(value.toString());
			@SuppressWarnings("resource")
			HTable table = new HTable(conf,"count".getBytes());
			Put put = new Put(Bytes.toBytes(time));
			put.add(Bytes.toBytes("r"),Bytes.toBytes("p"),Bytes.toBytes(sum));
			table.put(put);
		   /* String record = value.toString();
		    String mess [] = record.split("----");
		    String recordmess [] = mess[0].split("////");
		    HTable table = new HTable(conf,"sort".getBytes());
		    //System.out.println(key.toString());
		    Put put = new Put(Bytes.toBytes(key.toString()));
		    put.add(Bytes.toBytes("title"),Bytes.toBytes(recordmess[0]),Bytes.toBytes(mess[1]));
		    table.put(put);*/
		    
		    //将数据存储在 hbase 中，这次存储的是word与title的集合
		     
		    
		    
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
		}
		
	}

}