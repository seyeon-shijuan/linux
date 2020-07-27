package dataexpo;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ArrivalDelayCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private Text outkey = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Airline al = new Airline(value);
			outkey.set(al.getYear()+"," + al.getMonth());
			if(al.isArriveDelayAvailable() && al.getArriveDelayTime() >0) {
				//둘다 만족하는 경우에만 도착지연인것으로 간주되어 카운트가 올라간다.
				context.write(outkey, one);
			}
	}
}
