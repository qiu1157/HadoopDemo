/**   
* @Title: WordCount.java 
* @Package cn.com.pactera.hadoop 
* @Description: TODO
* @author xiangu.qiu@pactera.com
* @date 2014-12-14 下午8:54:14 
* @version V1.0   
*/
package cn.com.pactera.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/** 
 * @ClassName: WordCount 
 * @Description: TODO
 * @author xiangu.qiu@pactera.com
 * @date 2014-12-14 下午8:54:14 
 *  
 */
public class WordCount {

	/** 
	 * @Title: main 
	 * @Description: TODO(这里用一句话描述这个方法的作用) 
	 * @param @param args    设定文件 
	 * @return void    返回类型 
	 * @throws 
	 */
	/**
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 * @throws IOException  
	* @Title: main 
	* @Description: TODO(这里用一句话描述这个方法的作用) 
	* @param @param args    设定文件 
	* @return void    返回类型 
	* @throws 
	*/
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		if(otherArgs.length != 2) {
			System.out.println(otherArgs.length);
			System.err.println("Usage: wordcount<in><out>");
			System.exit(2);
		}
		Job job = new Job(conf,"WordCount");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
