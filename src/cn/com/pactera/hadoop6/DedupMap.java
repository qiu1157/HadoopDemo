/** 
* @Title: DedupMap.java 
* @Package cn.com.pactera.hadoop6 
* @Description: TODO
* @author xiangu.qiu@pactera.com
* @date 2014-12-29 下午3:38:55 
* @version V1.0   
*/
package cn.com.pactera.hadoop6;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** 
 * @ClassName: DedupMap 
 * @Description: TODO
 * @author xiangu.qiu@pactera.com
 * @date 2014-12-29 下午3:38:55 
 *  
 */
public class DedupMap extends Mapper<Object, Text, Text, Text> {
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		context.write(value, new Text(" "));
	}
}
