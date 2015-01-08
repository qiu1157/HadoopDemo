/** 
* @Title: MTjoinReduce.java 
* @Package cn.com.pactera.hadoop9 
* @Description: TODO
* @author xiangu.qiu@pactera.com
* @date 2015-1-3 下午12:50:34 
* @version V1.0   
*/
package cn.com.pactera.hadoop9;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/** 
 * @ClassName: MTjoinReduce 
 * @Description: TODO
 * @author xiangu.qiu@pactera.com
 * @date 2015-1-3 下午12:50:34 
 *  
 */
public class MTjoinReduce extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key , Iterable<Text> values , Context context) throws IOException, InterruptedException {
		 String[] factory = new String[10];
		 String[] address = new String[10];
		 int factorynum = 0;
		 int addressnum = 0;
		 for(Text var : values) {
			 System.out.println(var.toString());
			 String[] str = var.toString().split("\\+");
			 if("1".equals(str[0])) {
				 address[addressnum] = str[1];
				 addressnum++;
			 }else if("2".equals(str[0])) {
				 System.out.println("str=="+str[1]);
				 factory[factorynum] = str[1];
				 factorynum++;
			 }
		 }
		 if(0 != addressnum && 0 != factorynum) {
			 for(int i = 0 ; i < factorynum ; i++) {
				 for(int j = 0 ; j < addressnum ; j++) {
					 context.write(new Text(factory[i]), new Text(address[j]));
				 }
			 }
		 }
	}
}
