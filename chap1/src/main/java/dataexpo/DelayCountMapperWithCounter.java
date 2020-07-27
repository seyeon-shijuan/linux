package dataexpo;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class DelayCountMapperWithCounter extends Mapper<LongWritable, Text, Text, IntWritable> {
	private String workType;
	private final static IntWritable one = new IntWritable(1);
	private Text outkey = new Text();
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		workType = context.getConfiguration().get("workType");
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		Airline al = new Airline(value);
		context.getCounter(DelayCounters.total_count).increment(1);
		if (workType.equals("departure")) {
			if (al.isDepartureDelayAvailable()) {
				if (al.getDepartureDelayTime() > 0) {
					outkey.set(al.getYear() + ", " +al.getMonth());
					context.write(outkey, one); // 출발 지연 대상
					context.getCounter(DelayCounters.delay_departure).increment(1); // 리듀서로 전송할 데이터
				}else if (al.getDepartureDelayTime() == 0){
					context.getCounter(DelayCounters.scheduled_departure).increment(1); // 리듀서로 전송할 데이터
				}else if (al.getDepartureDelayTime() < 0) {
					context.getCounter(DelayCounters.early_departure).increment(1);
				}
			}else {
				context.getCounter(DelayCounters.not_available_departure).increment(1);
			}
		}else if (workType.equals("arrival")) {
			if (al.isArriveDelayAvailable()) {
				if (al.getArriveDelayTime() > 0) {
					outkey.set(al.getYear() + ", " +al.getMonth());
					context.write(outkey, one); // 출발 지연 대상
					context.getCounter(DelayCounters.delay_arrival).increment(1); // 리듀서로 전송할 데이터
				}else if (al.getArriveDelayTime() == 0){
					context.getCounter(DelayCounters.scheduled_arrival).increment(1);
				}else if (al.getArriveDelayTime() < 0) {
					context.getCounter(DelayCounters.early_arrival).increment(1);
				}
			}else {
				context.getCounter(DelayCounters.not_available_arrival).increment(1);
			}
		}else if (workType.equals("month")) {
			if (al.isDepartureDelayAvailable()) {
				if (al.getDepartureDelayTime() > 0) {
					outkey.set(al.getYear() + ", " +al.getMonth());
					context.write(outkey, one); // 출발 지연 대상
					context.getCounter(DelayCounters.delay_departure).increment(1); // 리듀서로 전송할 데이터
				}else if (al.getDepartureDelayTime() == 0){
					context.getCounter(DelayCounters.scheduled_departure).increment(1); // 리듀서로 전송할 데이터
				}else if (al.getDepartureDelayTime() < 0) {
					context.getCounter(DelayCounters.early_departure).increment(1);
				}
			}else {
				context.getCounter(DelayCounters.not_available_departure).increment(1);
			}
		}else if (workType.equals("carr")) {
			if (al.isDepartureDelayAvailable()) {
				if (al.getDepartureDelayTime() > 0) {
					outkey.set(al.getUniqueCarrier());
					context.write(outkey, one); // 출발 지연 대상
					context.getCounter(DelayCounters.delay_departure).increment(1); // 리듀서로 전송할 데이터
				}else if (al.getDepartureDelayTime() == 0){
					context.getCounter(DelayCounters.scheduled_departure).increment(1); // 리듀서로 전송할 데이터
				}else if (al.getDepartureDelayTime() < 0) {
					context.getCounter(DelayCounters.early_departure).increment(1);
				}
			}else {
				context.getCounter(DelayCounters.not_available_departure).increment(1);
			}
		}
		
	}
}
