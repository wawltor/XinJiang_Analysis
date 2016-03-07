package com.analysis.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.analysis.format.PeopleOutputFormat;

public class PeopleCountByday {
	public static class PeopleCountMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
		private Text keyinfo = new Text();
		private IntWritable one = new IntWritable(1);
		public  void map(LongWritable key , Text value , Context context) throws IOException, InterruptedException{
			//按天统计我们通过的人数
			String messages[] =  value.toString().split(",");
			String day = messages[0].substring(0, 8);
			System.out.println(day);
			keyinfo.set(day);
			context.write(keyinfo, one);
		}
	}
   
	//reduce
	public static class PeopleCountReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
		private IntWritable valueInfo = new  IntWritable();
		public void reduce(Text key , Iterable<IntWritable> values , Context context) throws IOException, InterruptedException{
			//统计我们的人数
			int sum =0;
			for(IntWritable value : values){
				sum += value.get();
			}
			valueInfo.set(sum);
			context.write(key, valueInfo);
		}
	}
	
	
	//设置我们的主程序
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
	
		//配置job
		Configuration conf = new Configuration();
		@SuppressWarnings("deprecation")
		Job job = new Job(conf,"app");
		//配置我们的类
		job.setJarByClass(PeopleCountByday.class);
		job.setMapperClass(PeopleCountMapper.class);
		job.setReducerClass(PeopleCountReducer.class);
		
		//设置我们输入输出的格式路径
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(PeopleOutputFormat.class);
		
		//设置我们的输入输出环境
		TextInputFormat.setInputPaths(job, new Path("hdfs://localhost:9000/user/analysis/"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/user/analysisou2/"));
		job.waitForCompletion(true);
		
		
	}

}
