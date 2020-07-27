package dataexpo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
// 출발지연, 도착지연을 한번에 처리하여 2개의 파일로 저장
public class DelayCountMultiOutput {
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		String in = "hdfs://localhost:9000/user/hadoop/dataexpo/1988.csv";
		String out = "1988out";
		Job job = new Job(new Configuration(), "DelayCountMultiOutput");
		FileInputFormat.addInputPath(job, new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(out));
		job.setJarByClass(DelayCountMultiOutput.class);
		job.setMapperClass(DelayCountMapperMultiOutput.class); // 맵 클래스 설정
		job.setReducerClass(DelayCountReducerMultiOutput.class); // 리듀서 클래스 설정
		job.setInputFormatClass(TextInputFormat.class); // 원본 데이터의 자료형 설정 : 문자형 데이터
		job.setOutputFormatClass(TextOutputFormat.class); // 결과 데이터의 자료형 설정 : 문자형 데이터
		job.setMapOutputKeyClass(Text.class); // key 자료형 저장 : 문자형 데이터
		job.setMapOutputValueClass(IntWritable.class); // value 자료형 저장 : 문자형 데이터
		// 결과물 파일을 여러개의 파일로 출력하도록 설정
		MultipleOutputs.addNamedOutput(job, "departure", TextOutputFormat.class, Text.class, IntWritable.class);
		MultipleOutputs.addNamedOutput(job, "arrival", TextOutputFormat.class, Text.class, IntWritable.class);
		job.waitForCompletion(true); // 작업 실행
		for(DelayCounters d : DelayCounters.values()) { // d : enum 객체
			long tot = job.getCounters().findCounter(d).getValue(); // long으로 변환
			System.out.println(d + " : " + tot);
		}
	}
	
	private static class DelayCountMapperMultiOutput extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text outkey = new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			Airline al = new Airline(value);
			if(al.isDepartureDelayAvailable()) {
				if (al.getDepartureDelayTime() > 0) {
					outkey.set("D,"+al.getYear() + ", " + al.getMonth());
					context.write(outkey, one);
				}
			}
			if (al.isArriveDelayAvailable()) {
				if (al.getArriveDelayTime() > 0) {
					outkey.set("A,"+al.getYear() + ", " + al.getMonth());
					context.write(outkey, one);
				}
			}
		}
	}
	
	public static class DelayCountReducerMultiOutput extends Reducer<Text, IntWritable, Text, IntWritable> {
		private MultipleOutputs <Text, IntWritable> mos;
		private Text outkey = new Text();
		private IntWritable result = new IntWritable();

		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			mos = new MultipleOutputs<Text, IntWritable>(context);
		}

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			// key : D, 1988, 01
			String [] columns = key.toString().split(",");
			outkey.set(columns[1] + ", " + columns[2]);
			int sum = 0;
			for(IntWritable v : values) sum += v.get();
			result.set(sum);
			if(columns[0].equals("D")) {
				mos.write("departure", outkey, result);
			}else if(columns[0].equals("A")) {
				mos.write("arrival", outkey, result);
			}
		}

		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			mos.close();
		}
		
	}
}
