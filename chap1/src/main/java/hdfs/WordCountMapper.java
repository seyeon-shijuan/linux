package hdfs;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//Mapper 클래스 : 맵의 기능을 처리, 원본데이터를 읽어서 매핑작업-> 결과는 리듀서의입력값

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	// 숫자 1
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	@Override
	//value: 값 this is a book
	public void map(LongWritable key, Text value, Context context) 
	//입력키타입, 입력값  타입, context 객체
			throws IOException, InterruptedException {
		//StringTokenizer: 문자열을 토큰화 해주는 클래스, 기본적 공백기준으로 분리된 문자열
		StringTokenizer itr = new StringTokenizer(value.toString());
		while(itr.hasMoreTokens()) {
			//this
			word.set(itr.nextToken());
			//context: 최종 결과값. 매퍼의 산출물
			context.write(word, one); // this :1
			// is, 1
			// a, 1
			// book, 1
		}
	}


}
