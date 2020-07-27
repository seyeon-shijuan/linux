package dataexpo;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// workType = month : 년도,월 을 key로 이용한 지연건수를 산출
// workType = carr : 항공사을 key로 이용한 지연건수를 산출
public class DelayCount2 extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
		String [] arg = {"-D", "workType=month", "hdfs://localhost:9000/user/hadoop/dataexpo/1987.csv",
				"departure-1987-month"};
		// -D :툴 이름
		//  departure-1988 : 출력 위치값
		int res = ToolRunner.run(new Configuration(), new DelayCount2(), arg);
		// Configuration : 환경변수, DelayCount2 : 객체, arg : 매개변수
		
	}

	@Override
	public int run(String[] args) throws Exception {
		String [] otherargs = new GenericOptionsParser(getConf(),args).getRemainingArgs();
		System.out.println(Arrays.toString(otherargs)); // [hdfs://localhost:9000/user/hadoop/dataexpo/1988.csv, departure-1988]
		if(otherargs.length != 2) {
			System.err.println("Usage: DelayCount2 <in> <out>");
			// System.err : 표준 오류 객체, 콘솔 출력
			System.exit(2); // 정상종료로 인식
		}
		Job job = new Job(getConf(), "DelayCount2");
		FileInputFormat.addInputPath(job, new Path(otherargs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherargs[1]));
		job.setJarByClass(DelayCount2.class);
		job.setMapperClass(DelayCountMapperWithCounter.class); // 맵 클래스 설정
		job.setReducerClass(DelayCountReducer.class); // 리듀서 클래스 설정
		job.setInputFormatClass(TextInputFormat.class); // 원본 데이터의 자료형 설정 : 문자형 데이터
		job.setOutputFormatClass(TextOutputFormat.class); // 결과 데이터의 자료형 설정 : 문자형 데이터
		job.setMapOutputKeyClass(Text.class); // key 자료형 저장 : 문자형 데이터
		job.setMapOutputValueClass(IntWritable.class); // value 자료형 저장 : 문자형 데이터
		job.waitForCompletion(true); // 작업 실행
		for(DelayCounters d : DelayCounters.values()) { // d : enum 객체
			long tot = job.getCounters().findCounter(d).getValue(); // long으로 변환
			System.out.println(d + " : " + tot);
		}
		return 0;
	}
}
