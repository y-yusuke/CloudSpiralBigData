package c1.big.data.mapreduce;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

import c1.big.data.util.CSKV;

public class C1Reducer_2nd extends Reducer<CSKV, CSKV, CSKV, CSKV> {
	protected void reduce(CSKV key, Iterable<CSKV> values, Context context)
			throws IOException, InterruptedException {
		long sum = 0;
		long n=0;
		for (CSKV value : values) {
			sum += value.toLong();
			n++;
		}
		double average = 1.0 * sum / n;
		context.write(key, new CSKV(average));
	}
}

