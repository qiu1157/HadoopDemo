/** 
* @Title: STMain.java 
* @Package cn.com.pactera.hadoop8 
* @Description: TODO
* @author xiangu.qiu@pactera.com
* @date 2015-1-1 下午9:51:08 
* @version V1.0   
*/
package cn.com.pactera.hadoop8;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/** 
 * @ClassName: STMain 
 * @Description: TODO
 * @author xiangu.qiu@pactera.com
 * @date 2015-1-1 下午9:51:08 
 *  
 */
public class STjoinMain {

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
		if(otherArgs.length !=2) {
			System.err.println("Usage: STMain<in><out>");
			System.exit(2);
		}
		Job job = new Job(conf,"STMain");
		job.setJarByClass(STjoinMain.class);
		job.setMapperClass(STjoinMapper.class);
		job.setReducerClass(STjoinReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}

}
