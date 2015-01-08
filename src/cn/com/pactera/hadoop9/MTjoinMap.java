/** 
* @Title: MTjoinMap.java 
* @Package cn.com.pactera.hadoop9 
* @Description: TODO
* @author xiangu.qiu@pactera.com
* @date 2015-1-2 下午11:32:04 
* @version V1.0   
*/
package cn.com.pactera.hadoop9;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** 
 * @ClassName: MTjoinMap 
 * @Description: TODO
 * @author xiangu.qiu@pactera.com
 * @date 2015-1-2 下午11:32:04 
 *  
 */
public class MTjoinMap extends Mapper<Object, Text, Text, Text> {
	public void map(Object key , Text value , Context context) throws IOException, InterruptedException{
		String line = value.toString();
		StringTokenizer itr = new StringTokenizer(line,"\n");
		while(itr.hasMoreTokens()) {
			String dline = itr.nextToken();
			//文件title不处理
			if( dline.contains("factoryname")||dline.contains("addressID")) {
				return;
			}
			//定义变量 i，记录factory表的addressed字段在line中的位置
			int i = 0;
			while (dline.charAt(i)>='9' || dline.charAt(i)<='0') {
				i++;
			}
			//如果line第一个字符是数字,则为address表的数据
			if(dline.charAt(0)<='9' && dline.charAt(0)>='0') {
				String[] str = dline.split(" ");
				String addressID = str[0];
				String addressname = str[1];
				context.write(new Text(addressID), new Text("1+"+addressname));
			}else{
				String factoryname = dline.substring(0,i);
				String addressID = dline.substring(line.length()-1);
				context.write(new Text(addressID), new Text("2+"+factoryname));
			}
		}
	}
}
