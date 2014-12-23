/** 
 * @Title: FilCopy.java 
 * @Package cn.com.pactera.hadoop3 
 * @Description: TODO
 * @author xiangu.qiu@pactera.com
 * @date 2014-12-23 上午11:33:06 
 * @version V1.0   
 */
package cn.com.pactera.hadoop3;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * @ClassName: HadoopFileDemo
 * @Description: TODO
 * @author xiangu.qiu@pactera.com
 * @date 2014-12-23 下午2:54:22
 * 
 */
public class HadoopFileDemo {

	/**
	 * @Title: FileCopy
	 * @Description: TODO(这里用一句话描述这个方法的作用)
	 * @param @param args
	 * @param @throws IOException 设定文件
	 * @return void 返回类型
	 * @throws
	 */
	public void FileCopy(String source, String target) throws IOException {
		Configuration conf = new Configuration();
		InputStream in = new BufferedInputStream(new FileInputStream(source));
		FileSystem fs = FileSystem.get(URI.create(target), conf);
		OutputStream out = fs.create(new Path(target));
		IOUtils.copyBytes(in, out, 4096, true);
	}

	/**
	 * @Title: FileDelete
	 * @Description: TODO(这里用一句话描述这个方法的作用)
	 * @param @param file
	 * @param @throws IOException 设定文件
	 * @return void 返回类型
	 * @throws
	 */
	public void FileDelete(String file) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(file), conf);
		fs.delete(new Path(file), false);
	}

	public void FileCat(String path) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(path), conf);
		InputStream in = null;
		in = fs.open(new Path(path));
		try {
			IOUtils.copyBytes(in, System.out, 4096, false);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(in);
		}
	}
	
	public void FileInfo(String file) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(file), conf);
		FileStatus stat = fs.getFileStatus(new Path(file));
		System.out.println(stat.getPath());
		System.out.println(stat.getLen());
		System.out.println(stat.getModificationTime());
		System.out.println(stat.getOwner());
		System.out.println(stat.getReplication());
		System.out.println(stat.getBlockSize());
		System.out.println(stat.getGroup());
		System.out.println(stat.getPermission());
	}

	public void FileList(String path) throws IOException{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(path), conf);
		FileStatus[] status  = fs.listStatus(new Path(path));
		Path[] listedPaths = FileUtil.stat2Paths(status);
		for(Path p : listedPaths) {
			System.out.println(p);
		}
	}
	/**
	 * @throws IOException
	 * @throws FileNotFoundException
	 * @Title: main
	 * @Description: TODO(这里用一句话描述这个方法的作用)
	 * @param @param args 设定文件
	 * @return void 返回类型
	 * @throws
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		if (args.length != 1) {
			System.err.println("Usage: filecopy<source><target>");
			System.exit(2);
		}
		// new HadoopFileDemo().FileCopy(args[0],args[1]);
		//new HadoopFileDemo().FileDelete(args[0]);
		//new HadoopFileDemo().FileCat(args[0]);
		//new HadoopFileDemo().FileInfo(args[0]);
		new HadoopFileDemo().FileList(args[0]);
	}

}
