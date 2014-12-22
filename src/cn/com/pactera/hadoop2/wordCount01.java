/** 
* @Title: wordCount01.java 
* @Package cn.com.pactera.hadoop2 
* @Description: TODO
* @author xiangu.qiu@pactera.com
* @date 2014-12-22 上午11:25:05 
* @version V1.0   
*/
package cn.com.pactera.hadoop2;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/** 
 * @ClassName: wordCount01 
 * @Description: TODO
 * @author xiangu.qiu@pactera.com
 * @date 2014-12-22 上午11:25:05 
 *  
 */
public class wordCount01 {

	/** 
	* @ClassName: TokenizerMapper 
	* @Description: TODO
	* @author xiangu.qiu@pactera.com
	* @date 2014-12-22 上午11:33:10 
	*  
	*/
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		/* (非 Javadoc) 
		* <p>Title: map</p> 
		* <p>Description: </p> 
		* @param key
		* @param value
		* @param context
		* @throws IOException
		* @throws InterruptedException 
		* @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context) 
		*/
		public void map(Object key,Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while(itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}
	
	/** 
	* @ClassName: IntSumReducer 
	* @Description: TODO
	* @author xiangu.qiu@pactera.com
	* @date 2014-12-22 上午11:38:23 
	*  
	*/
	public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();
		/** 
		* @Title: reducer 
		* @Description: TODO(这里用一句话描述这个方法的作用) 
		* @param @param key
		* @param @param values
		* @param @param context
		* @param @throws IOException
		* @param @throws InterruptedException    设定文件 
		* @return void    返回类型 
		* @throws 
		*/
		public void reducer(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0 ;
			for(IntWritable val : values) {
				sum +=val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	/**
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
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
		if(otherArgs.length != 2){
			System.out.println("Usage: wordcount01<in><out>");
			System.exit(2);
		}
		
		Job job = new Job(conf,"word count");
		job.setJarByClass(wordCount01.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}

}
