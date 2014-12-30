/** 
* @Title: SortMap.java 
* @Package cn.com.pactera.hadoop7 
* @Description: TODO
* @author xiangu.qiu@pactera.com
* @date 2014-12-30 上午10:16:33 
* @version V1.0   
*/
package cn.com.pactera.hadoop7;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** 
 * @ClassName: SortMap 
 * @Description: TODO
 * @author xiangu.qiu@pactera.com
 * @date 2014-12-30 上午10:16:33 
 *  
 */
public class SortMap extends Mapper<Object , Text , IntWritable , IntWritable>{
	public void map(Object key , Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		IntWritable data = new IntWritable();
		data.set(Integer.parseInt(line));
		context.write(data, new IntWritable(1));
	}
}
