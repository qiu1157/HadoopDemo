package cn.com.pactera.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @ClassName: TokenizerMapper
 * @Description: TODO
 * @author xiangu.qiu@pactera.com
 * @date 2014-12-14 下午5:55:58
 * 第一个参数Object 表示key的参数类型
 * 第二个参数Text 表示输入值的类型
 * 第三个参数Text 表示输出健类型
 * 第四个参数IntWritable 表示输出值类型
 */
public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

	IntWritable iw = new IntWritable(1);
	//Text是hadoop中存储字符串的可比较可序列化类
	Text word = new Text();

	/*
	 * (非 Javadoc) <p>Title: map</p> <p>Description: </p>
	 * 
	 * @param key 输入键，是什么无所谓，实际上用不到它
	 * 
	 * @param value 输入值，在map函数中出错会抛出异常
	 * 
	 * @param context 咋看Mapper内部定义，不需要引入，继承MapContext类
	 * 
	 * @throws IOException
	 * 
	 * @throws InterruptedException
	 * 
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
	 * org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()) {
			word.set(itr.nextToken());
			System.out.println("word="+word+"****"+"iw="+iw);
			context.write(word, iw);
		}
	}
}
