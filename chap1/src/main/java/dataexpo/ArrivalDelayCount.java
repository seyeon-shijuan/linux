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

public class ArrivalDelayCount {
//드라이버 클래스: 맵리듀스 잡에 대한 실행정보를 설정하고, 맴리듀스 잡을 실행한다.
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		String in = "hdfs://localhost:9000/user/hadoop/dataexpo/1988.csv";
		String out = "hdfs://localhost:9000/user/hadoop/dataexpo/1988out";
		//잡객체가 맵 리듀스 잡을 실행한다.
		Job job = new Job(conf,"ArrivalDelayCount");
		
		//어떤 경로로 전달받은 건지 설정
		FileInputFormat.addInputPath(job, new Path(in)); //입력 파라미터의 경로
		FileOutputFormat.setOutputPath(job, new Path(out)); //출력 파라미터가 생성될 경로
		
		job.setJarByClass(ArrivalDelayCount.class); //작업 클래스 설정 : 실행하면 이걸 실행하는 거로 설정한거임
		
		//job에서 사용할 매퍼 클래스와 리듀서 클래스 설정
		job.setMapperClass(ArrivalDelayCountMapper.class);
		job.setReducerClass(DelayCountReducer.class);
		
		//입출력 모두 텍스트 파일을 사용해서
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//매퍼와 리듀서 클래스의 출력데이터의 키와 값의 타입 설정
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//이게 잡을 실행하는거
		job.waitForCompletion(true);
	}

}
