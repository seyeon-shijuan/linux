package hdfs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
	private IntWritable result = new IntWritable();
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
	//values :입력파라미터의 값
	throws IOException, InterruptedException {
		int sum = 0;
		//입력 파라미터의 값에 담겨 있는 글자 수를 합산하기위해 선언
		for(IntWritable v: values) { //1, 1
			sum += v.get();
		}
		result.set(sum); //2
		//출력값 설정
		context.write(key, result); //this,2
		//is,2
		//a,2
		//book,1
		//pen,1
	}

}
