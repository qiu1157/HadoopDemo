/** 
* @Title: NumPair.java 
* @Package cn.com.pactera.hadoop10 
* @Description: TODO
* @author xiangu.qiu@pactera.com
* @date 2015-1-9 下午3:58:11 
* @version V1.0   
*/
package cn.com.pactera.hadoop10;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

/** 
 * @ClassName: NumPair 
 * @Description: TODO
 * @author xiangu.qiu@pactera.com
 * @date 2015-1-9 下午3:58:11 
 *  
 */
public class NumPair implements WritableComparable<NumPair> {

	private LongWritable line;
	private LongWritable location;
	public NumPair() {
		set (new LongWritable(0), new LongWritable(0));
	}
	
	
	/** 
	* @Title: set 
	* @Description: TODO(这里用一句话描述这个方法的作用) 
	* @param @param longWritable
	* @param @param longWritable2    设定文件 
	* @return void    返回类型 
	* @throws 
	*/
	public void set(LongWritable first, LongWritable second) {
		// TODO Auto-generated method stub
		this.line = first;
		this.location = second;
	}
	
	public NumPair(LongWritable first, LongWritable second) {
		set(first, second);
	}
	
	public NumPair(int first, int second) {
		set(new LongWritable(first), new LongWritable(second));
	}
	
	public LongWritable getLine() {
		return line;
	}
	
	public LongWritable getLocation() {
		return location;
	}
	
	/* (非 Javadoc) 
	 * <p>Title: write</p> 
	 * <p>Description: </p> 
	 * @param out
	 * @throws IOException 
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput) 
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		line.write(out);
		location.write(out);
	}

	/* (非 Javadoc) 
	 * <p>Title: readFields</p> 
	 * <p>Description: </p> 
	 * @param in
	 * @throws IOException 
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput) 
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		line.readFields(in);
		location.readFields(in);
	}

	/* (非 Javadoc) 
	 * <p>Title: compareTo</p> 
	 * <p>Description: </p> 
	 * @param o
	 * @return 
	 * @see java.lang.Comparable#compareTo(java.lang.Object) 
	 */
	@Override
	public int compareTo(NumPair o) {
		// TODO Auto-generated method stub
		if((this.line == o.line) && (this.location == o.location)) {
			return 0;
		}
		return -1;
	}

	public boolean equals(NumPair o) {
		if((this.line == o.line) && (this.location == o.location)) {
			return true;
		}
		return false;		
	}

}
