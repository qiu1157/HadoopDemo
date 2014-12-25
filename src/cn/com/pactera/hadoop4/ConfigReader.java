/** 
* @Title: ConfigReader.java 
* @Package cn.com.pactera.hadoop4 
* @Description: TODO
* @author xiangu.qiu@pactera.com
* @date 2014-12-23 下午4:50:51 
* @version V1.0   
*/
package cn.com.pactera.hadoop4;

import org.apache.hadoop.conf.Configuration;

/** 
 * @ClassName: ConfigReader 
 * @Description: TODO
 * @author xiangu.qiu@pactera.com
 * @date 2014-12-23 下午4:50:51 
 *  
 */
public class ConfigReader {

	/** 
	 * @Title: main 
	 * @Description: TODO(这里用一句话描述这个方法的作用) 
	 * @param @param args    设定文件 
	 * @return void    返回类型 
	 * @throws 
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		System.out.println(conf.get("hadoop.tmp.dir"));
		System.out.println(conf.get("mapred.job.tracker"));
	}

}
