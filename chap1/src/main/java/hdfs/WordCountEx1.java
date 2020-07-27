package hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class WordCountEx1 {

	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job job = new Job(conf, "WordCount"); //작업 설정
		job.setJarByClass(WordCountEx1.class); //작업 클래스 설정
		job.setMapperClass(WordCountMapper.class); //맵클래스 설정
		job.setReducerClass(WordCountReducer.class); //리듀서클래스 설정
		job.setInputFormatClass(TextInputFormat.class); 
		//원본 데이터의 자료형을 설정 : 문자형 데이터
		job.setOutputFormatClass(TextOutputFormat.class);
		//결과 데이터의 자료형을 설정 : 문자형 데이터
		job.setMapOutputKeyClass(Text.class);
		//key의 자료형 지정 : 문자형 데이터
		job.setMapOutputValueClass(IntWritable.class);
		//value의 자료형 지정 : 정수형 데이터
		FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/user/hadoop/in"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/user/hadoop/wordcnt"));
		job.waitForCompletion(true); //작업 실행
	}

}
