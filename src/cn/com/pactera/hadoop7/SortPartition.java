/** 
* @Title: Partition.java 
* @Package cn.com.pactera.hadoop7 
* @Description: TODO
* @author xiangu.qiu@pactera.com
* @date 2014-12-30 上午10:51:44 
* @version V1.0   
*/
package cn.com.pactera.hadoop7;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/** 
 * @ClassName: Partition 
 * @Description: TODO
 * @author xiangu.qiu@pactera.com
 * @date 2014-12-30 上午10:51:44 
 *  
 */
public class SortPartition extends Partitioner<IntWritable, IntWritable> {

	/* (非 Javadoc) 
	* <p>Title: getPartition</p> 
	* <p>Description: </p> 
	* @param arg0
	* @param arg1
	* @param arg2
	* @return 
	* @see org.apache.hadoop.mapreduce.Partitioner#getPartition(java.lang.Object, java.lang.Object, int) 
	*/
	@Override
	public int getPartition(IntWritable key, IntWritable value, int unmPartitions) {
		// TODO Auto-generated method stub
		int Maxnumber=65223;
		int bound = Maxnumber/unmPartitions+1 ;
		int keynumber = key.get() ; 
		for(int i = 1; i < unmPartitions ; i++) {
			if(keynumber<bound*i && keynumber>=bound*(i-1))
				return i-1;
		}
		return 0;
	}

}
