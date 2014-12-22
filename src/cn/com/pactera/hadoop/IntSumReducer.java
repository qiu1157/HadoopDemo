/**   
 * @Title: IntSumReducer.java 
 * @Package cn.com.pactera.hadoop 
 * @Description: TODO
 * @author xiangu.qiu@pactera.com
 * @date 2014-12-14 下午8:22:30 
 * @version V1.0   
 */
package cn.com.pactera.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @ClassName: IntSumReducer
 * @Description: TODO
 * @author xiangu.qiu@pactera.com
 * @date 2014-12-14 下午8:22:30
 * 
 */
public class IntSumReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	IntWritable result = new IntWritable();

	/*
	 * (非 Javadoc) <p>Title: reduce</p> <p>Description: </p>
	 * 
	 * @param arg0
	 * 
	 * @param arg1
	 * 
	 * @param arg2
	 * 
	 * @param arg3
	 * 
	 * @throws IOException
	 * 
	 * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object,
	 * java.util.Iterator, org.apache.hadoop.mapred.OutputCollector,
	 * org.apache.hadoop.mapred.Reporter)
	 */
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		result.set(sum);
		context.write(key, result);
	}

}
