/** 
* @Title: SortReduce.java 
* @Package cn.com.pactera.hadoop7 
* @Description: TODO
* @author xiangu.qiu@pactera.com
* @date 2014-12-30 上午10:32:52 
* @version V1.0   
*/
package cn.com.pactera.hadoop7;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/** 
 * @ClassName: SortReduce 
 * @Description: TODO
 * @author xiangu.qiu@pactera.com
 * @date 2014-12-30 上午10:32:52 
 *  
 */
public class SortReduce extends Reducer<IntWritable , IntWritable , IntWritable , IntWritable>{
	private static IntWritable linenum = new IntWritable(1);
	public void reduce(IntWritable key , Iterable <IntWritable> values , Context context ) throws IOException, InterruptedException {	
		for(IntWritable val : values) {
			context.write(linenum, key);
			linenum = new IntWritable(linenum.get()+1);
		}
	}
}
