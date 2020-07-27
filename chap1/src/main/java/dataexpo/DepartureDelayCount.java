package dataexpo;

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

public class DepartureDelayCount {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String in = "hdfs://localhost:9000/user/hadoop/dataexpo/1988.csv";
		String out = "hdfs://localhost:9000/user/hadoop/dataexpo/1988out2";
		Job job = new Job(conf, "DepartureDelayCount");
		FileInputFormat.addInputPath(job, new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(out));
		job.setJarByClass(DepartureDelayCount.class);
		job.setMapperClass(DepartureDelayCountMapper.class); // 맵 클래스 설정
		job.setReducerClass(DelayCountReducer.class); // 리듀서 클래스 설정
		job.setInputFormatClass(TextInputFormat.class); // 원본 데이터의 자료형 설정 : 문자형 데이터
		job.setOutputFormatClass(TextOutputFormat.class); // 결과 데이터의 자료형 설정 : 문자형 데이터
		job.setMapOutputKeyClass(Text.class); // key 자료형 저장 : 문자형 데이터
		job.setMapOutputValueClass(IntWritable.class); // value 자료형 저장 : 문자형 데이터
		job.waitForCompletion(true); // 작업 실행
	}
}
