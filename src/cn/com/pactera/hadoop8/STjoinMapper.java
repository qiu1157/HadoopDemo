/** 
 * @Title: STjoinMapper.java 
 * @Package cn.com.pactera.hadoop8 
 * @Description: TODO
 * @author xiangu.qiu@pactera.com
 * @date 2014-12-31 上午11:20:44 
 * @version V1.0   
 */
package cn.com.pactera.hadoop8;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @ClassName: STjoinMapper
 * @Description: TODO
 * @author xiangu.qiu@pactera.com
 * @date 2014-12-31 上午11:20:44
 * 
 */
public class STjoinMapper extends Mapper<Object, Text, Text, Text> {
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String childName = new String();
		String parentName = new String();
		String relationType = new String();
		String line = value.toString();
		StringTokenizer itr = new StringTokenizer(line,"\n");
		while (itr.hasMoreTokens()) {
			StringTokenizer tokenLine = new StringTokenizer(itr.nextToken());
			childName = tokenLine.nextToken();
			parentName = tokenLine.nextToken();
			if (childName.compareTo("child") != 0) {
				// 左右表区分标志 1为左表 2为右表
				relationType = "1";
				// 插入左表
				context.write(new Text(parentName), new Text(relationType + "+"
						+ childName));
				relationType = "2";
				// 插入右表
				context.write(new Text(childName), new Text(relationType + "+"
						+ parentName));
			}
		}
	}
}
