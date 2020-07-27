package dataexpo;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class CarrDepartureDelayCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text outkey = new Text();
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		Airline al = new Airline(value);
		outkey.set(al.getUniqueCarrier());
		if (al.isDepartureDelayAvailable() && al.getDepartureDelayTime() > 0) {
			context.write(outkey, one); // 출발 지연 대상
		}
	}
}
