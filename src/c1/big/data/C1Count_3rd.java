package c1.big.data;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import c1.big.data.mapreduce.C1Mapper_3rd;
import c1.big.data.mapreduce.C1Reducer_3rd;
import c1.big.data.util.CSKV;
import c1.big.data.util.PosUtils;

public class C1Count_3rd {
	public static String InputPath = "posdata";
	public static String OutputPath = "out/C1Count3rd";
	public static int TaskNum = 8;

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		Job job = new Job(new Configuration());
		job.setJarByClass(C1Count_3rd.class);
		job.setMapperClass(C1Mapper_3rd.class);
		job.setReducerClass(C1Reducer_3rd.class);
		job.setJobName("C1BigData");

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(CSKV.class);
		job.setMapOutputValueClass(CSKV.class);
		job.setOutputKeyClass(CSKV.class);
		job.setOutputValueClass(CSKV.class);

		String inputpath = InputPath;
		String outputpath = OutputPath; // ★MRの出力先
		if (args.length > 0) {
			inputpath = args[0];
		}

		FileInputFormat.setInputPaths(job, new Path(inputpath));
		FileOutputFormat.setOutputPath(job, new Path(outputpath));

		// 出力フォルダは実行の度に毎回削除する（上書きエラーが出るため）
		PosUtils.deleteOutputDir(outputpath);

		// Reducerで使う計算機数を指定
		job.setNumReduceTasks(TaskNum);

		// MapReduceジョブを投げ，終わるまで待つ．
		job.waitForCompletion(true);

		System.out.println("Finish 3rd");
	}
}
