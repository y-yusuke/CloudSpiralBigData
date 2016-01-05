package c1.big.data.mapreduce;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

import c1.big.data.Attr.Attr;
import c1.big.data.util.CSKV;

public class C1Reducer_1st extends Reducer<CSKV, CSKV, CSKV, CSKV> {
	protected void reduce(CSKV key, Iterable<CSKV> values, Context context)
			throws IOException, InterruptedException {
		int drinkCount = 0;
		int foodCount = 0;
		long sum = 0;
		for (CSKV value : values) {
			String[] v = value.split(Attr.SEPARATOR);
			String kind = v[0];
			String val = v[1];

			if(isDrink(kind)){
				sum += Long.valueOf(val);
				drinkCount++;
			}else if(isFood(kind)){
				sum += Long.valueOf(val);
				foodCount++;
			}
		}

		if(drinkCount != 0 && foodCount != 0)
			context.write(key,new CSKV(sum));
	}

	private boolean isFood(String kind) {
		return kind.equals(Attr.FOOD);
	}

	private boolean isDrink(String kind) {
		return kind.equals(Attr.DRINK);
	}
}
