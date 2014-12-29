/** 
* @Title: DedupReduce.java 
* @Package cn.com.pactera.hadoop6 
* @Description: TODO
* @author xiangu.qiu@pactera.com
* @date 2014-12-29 下午3:49:01 
* @version V1.0   
*/
package cn.com.pactera.hadoop6;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/** 
 * @ClassName: DedupReduce 
 * @Description: TODO
 * @author xiangu.qiu@pactera.com
 * @date 2014-12-29 下午3:49:01 
 *  
 */
public class DedupReduce extends Reducer<Text, Text, Text, Text>{
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		context.write(key, new Text(" "));
	}
}
