/** 
* @Title: ScoreReduce.java 
* @Package cn.com.pactera.hadoop5 
* @Description: TODO
* @author xiangu.qiu@pactera.com
* @date 2014-12-25 下午5:18:03 
* @version V1.0   
*/
package cn.com.pactera.hadoop5;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/** 
 * @ClassName: ScoreReduce 
 * @Description: TODO
 * @author xiangu.qiu@pactera.com
 * @date 2014-12-25 下午5:18:03 
 *  
 */
public class ScoreReduce extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce (Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		int count = 0;
		for(IntWritable val : values) {
			sum += val.get(); //计算总分
			count++; //计算科目
		}
		int avg = (int)sum/count; //计算平均成绩
		context.write(key, new IntWritable(avg));
	}

}
