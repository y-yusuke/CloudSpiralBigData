package c1.big.data.mapreduce;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import c1.big.data.Attr.Attr;
import c1.big.data.util.CSKV;
import c1.big.data.util.PosUtils;

public class C1Mapper_1st extends Mapper<LongWritable, Text, CSKV, CSKV> {
	protected void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
		// csvファイルをカンマで分割して，配列に格納する
		String[] csv = value.toString().split(",");
		if (!isCorrectReceipt(csv)) return;

		String kind;
		String code = getItemCode(csv[PosUtils.ITEM_CATEGORY_CODE]);
		// 食べ物 か 飲み物 の購入のみ
		if (isFoodCode(code)) kind = Attr.FOOD;
		else if (isDrinkCode(code)) kind = Attr.DRINK;
		else return;

		String id = csv[PosUtils.RECEIPT_ID];
		String val = csv[PosUtils.ITEM_PRICE];

		StringBuilder strBuil = new StringBuilder();
		strBuil.append(kind).append(Attr.SEPARATOR);
		strBuil.append(val);

		// key: ID , val: 種類:値段
		context.write(new CSKV(id), new CSKV(strBuil.toString()));
	}

	/**
	 * オフィス街
	 * 男性・大人
	 * 平日
	 * 11:00~13:30
	 * @param csv
	 * @return true of false
	 */
	private boolean isCorrectReceipt(String[] csv) {
		if (!isOfficeLocation(csv[PosUtils.LOCATION])) return false;
		if (!isSalaryman(csv[PosUtils.BUYER_SEX],csv[PosUtils.BUYER_AGE])) return false;
		if (!isWeekDay(csv[PosUtils.WEEK],csv[PosUtils.IS_HOLIDAY])) return false;
		if (!isLunchTime(Integer.valueOf(csv[PosUtils.HOUR]),Integer.valueOf(csv[PosUtils.MINUTE]))) return false;
		//if(Integer.parseInt(csv[PosUtils.YEAR]) < 2008) return false;
		return true;
	}

	/**
	 * オフィス街
	 * @param location
	 * @return true of false
	 */
	private boolean isOfficeLocation(String location) {
		return location.startsWith("オフィス街");
	}

	/**
	 * 男性:1(女性:2) かつ 大人
	 * @param sex
	 * @param age
	 * @return true of false
	 */
	private boolean isSalaryman(String sex,String age) {
		return sex.equals("2") && age.equals("3");
	}

	/**
	 * 土 or 日
	 * @param week
	 * @param holiday
	 * @return true of false
	 */
	private boolean isWeekDay(String week,String holiday) {
		if (week.equals("6") || week.equals("7")) return false;
		return !holiday.equals("1");
	}

	/**
	 * 11:00 ~ 13:30
	 * @param hour
	 * @param min
	 * @return true of false
	 */
	private boolean isLunchTime(int hour,int min) {
		if (11 <= hour && hour <= 13) {
			if (hour == 13) {
				if (31 <= min) return false;
			}
			return true;
		}
		return false;
	}

	private String getItemCode(String code) {
		if (code.length() == 6) return code.substring(0, 3);
		else throw new Error("mapper: code length error");
	}

	private boolean isFoodCode(String code) {
		String[] foodCode = FoodCode.code;
		return Arrays.asList(foodCode).contains(code);
	}

	private boolean isDrinkCode(String code) {
		String[] drinkCode = DrinkCode.code;
		return Arrays.asList(drinkCode).contains(code);
	}
}
