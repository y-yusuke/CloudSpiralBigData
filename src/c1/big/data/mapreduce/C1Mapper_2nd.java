package c1.big.data.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import c1.big.data.util.CSKV;

public class C1Mapper_2nd extends Mapper<LongWritable, Text, CSKV, CSKV> {
	protected void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
		// csvファイルをカンマで分割して，配列に格納する
		String[] csv = value.toString().split("\t");
		String[] receiptId = csv[0].split("-");
		String year = receiptId[0];
		String sum = csv[1];

		// key: year , val: 客単価
		context.write(new CSKV(year), new CSKV(sum));
	}
}