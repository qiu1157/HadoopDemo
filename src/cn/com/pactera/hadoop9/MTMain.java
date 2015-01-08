/** 
 * @Title: MTMain.java 
 * @Package cn.com.pactera.hadoop9 
 * @Description: TODO
 * @author xiangu.qiu@pactera.com
 * @date 2015-1-3 下午1:36:55 
 * @version V1.0   
 */
package cn.com.pactera.hadoop9;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @ClassName: MTMain
 * @Description: TODO
 * @author xiangu.qiu@pactera.com
 * @date 2015-1-3 下午1:36:55
 * 
 */
public class MTMain {

	/**
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 * @throws IOException
	 * @Title: main
	 * @Description: TODO(这里用一句话描述这个方法的作用)
	 * @param @param args 设定文件
	 * @return void 返回类型
	 * @throws
	 */
	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: STMain<in><out>");
			System.exit(2);
		}
		Job job = new Job(conf, "MTjoin");
		job.setJarByClass(MTMain.class);
		job.setMapperClass(MTjoinMap.class);
		job.setReducerClass(MTjoinReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
