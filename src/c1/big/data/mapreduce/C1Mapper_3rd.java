package c1.big.data.mapreduce;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import c1.big.data.util.CSKV;
import c1.big.data.util.PosUtils;

public class C1Mapper_3rd extends Mapper<LongWritable, Text, CSKV, CSKV> {
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
			// csvファイルをカンマで分割して，配列に格納する
			String[] csv = value.toString().split(",");
			if (!isCorrectReceipt(csv)) return;

			String id = csv[PosUtils.ITEM_NAME];
			String val = csv[PosUtils.ITEM_PRICE];

			// key: ID , val: 種類:値段
			context.write(new CSKV(id), new CSKV(val));
		}

		/**
		 * 菓子
		 * 2012年
		 * 150-155円 or 195-200
		 * @param csv
		 * @return true of false
		 */
		private boolean isCorrectReceipt(String[] csv) {
			if (!isSweets(getItemCode(csv[PosUtils.ITEM_CATEGORY_CODE]))) return false;
			if(Integer.parseInt(csv[PosUtils.YEAR]) != 2012) return false;
			if(Integer.parseInt(csv[PosUtils.ITEM_PRICE]) > 200 || Integer.parseInt(csv[PosUtils.ITEM_PRICE]) < 195) return false;
			return true;
		}

		private boolean isSweets(String code) {
			String[] proposeCode = ProposeCode.code;
			return Arrays.asList(proposeCode).contains(code);
		}

		private String getItemCode(String code) {
			if (code.length() == 6) return code.substring(0, 3);
			else throw new Error("mapper: code length error");
		}
}
