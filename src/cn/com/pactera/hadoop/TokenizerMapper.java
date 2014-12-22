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
 * 
 */
public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

	IntWritable iw = new IntWritable(1);
	Text word = new Text();

	/*
	 * (非 Javadoc) <p>Title: map</p> <p>Description: </p>
	 * 
	 * @param key
	 * 
	 * @param value
	 * 
	 * @param context
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
