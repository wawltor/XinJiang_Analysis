package com.analysis.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;



public class InsertDataTOHbase {
	   
	 public static String COLUMNS[] = {"Name","Sex","Nation","Birthday","Address","CardNO","SignGov","LimitedDate","EntryTime","Dubious","Photo","EntryPosition","EntryTodayTimes","EventCode","DeviceName","UploadFlag","DeviceIP","JingZhong","CreateTime","Destination","Phone","Verify","Remark","Direction","CheckResult"};
	//main
	public static class InsertMapper extends Mapper<LongWritable,Text, ImmutableBytesWritable, Put>{
		//map方法
		public void map(LongWritable key , Text value , Context context) throws IOException, InterruptedException{
			  String[] cloumnVals = value.toString().split(",");
		      String rowkey = cloumnVals[0].trim();
		      Put put = new Put(Bytes.toBytes(rowkey));
		      for (int i = 0; i < cloumnVals.length-1; i++) {
		    	put.add(Bytes.toBytes("d"),Bytes.toBytes(COLUMNS[i]),Bytes.toBytes(cloumnVals[i+1]));
		      }
		      context.write(new ImmutableBytesWritable(rowkey.getBytes()), put);
		}
		
	}
	//
	 @SuppressWarnings("deprecation")
	  public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
	    Configuration conf = new Configuration();
	    conf.set("hbase.zookeeper.quorum", "localhost");
	    Job job = new Job(conf,"hbase");
	    job.setJarByClass(InsertDataTOHbase.class);
	    job.setMapperClass(InsertMapper.class);
	    job.setNumReduceTasks(0);
	    job.setOutputFormatClass(TableOutputFormat.class);
	    job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "record");
	    FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/user/analysis/data.txt"));
	    System.out.println(job.waitForCompletion(true) ? 0 : 1);
	  }
	}


