/** 
 * @Title: STjoinReduce.java 
 * @Package cn.com.pactera.hadoop8 
 * @Description: TODO
 * @author xiangu.qiu@pactera.com
 * @date 2014-12-31 下午3:35:36 
 * @version V1.0   
 */
package cn.com.pactera.hadoop8;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @ClassName: STjoinReduce
 * @Description: TODO
 * @author xiangu.qiu@pactera.com
 * @date 2014-12-31 下午3:35:36
 * 
 */
public class STjoinReduce extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String grandchild[] = new String[10];
		String grandparent[] = new String[10];
		int grandchildnum = 0;
		int grandparentnum = 0;
		for (Text val : values) {
			String record = val.toString();
			String[] str = record.split("+");
			if ("1".equals(str[0])) {
				grandchild[grandchildnum] = str[1];
				grandchildnum++;
			} else if ("2".equals(str[0])) {
				grandparent[grandparentnum] = str[1];
				grandparentnum++;
			}
			if(0 !=grandchildnum && 0 != grandparentnum ) {
				for(int i = 0 ; i<grandchildnum; i++ ) {
					for(int j = 0 ; j<grandparentnum;  j++ ) {
						context.write(new Text(grandchild[i]), new Text(grandparent[j]) );
					}
				}
			}
		}
	}
}
