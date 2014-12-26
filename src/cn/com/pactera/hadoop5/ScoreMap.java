/** 
* @Title: Map.java 
* @Package cn.com.pactera.hadoop5 
* @Description: TODO
* @author xiangu.qiu@pactera.com
* @date 2014-12-25 上午11:24:21 
* @version V1.0   
*/
package cn.com.pactera.hadoop5;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** 
 * @ClassName: Map 
 * @Description: TODO
 * @author xiangu.qiu@pactera.com
 * @date 2014-12-25 上午11:24:21 
 *  
 */
public class ScoreMap extends Mapper<LongWritable , Text , Text, IntWritable>{
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
	public void map(LongWritable key , Text value , Context context) throws IOException, InterruptedException {
		String line = value.toString();
		System.out.println(line);
		StringTokenizer token = new StringTokenizer(line, "\n");
		while (token.hasMoreTokens()) {
			StringTokenizer tokenLine = new StringTokenizer(token.nextToken());
			String strName = tokenLine.nextToken();
			String strScore = tokenLine.nextToken();
			Text name = new Text(strName);
			IntWritable score = new IntWritable(Integer.parseInt(strScore));
			context.write(name, score);
		}
	}

}
