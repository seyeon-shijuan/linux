package hdfs;

import java.io.FileInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class WriteHadoopFile2 {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/hadoop-2.9.2/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/hadoop-2.9.2/etc/hadoop/hdfs-site.xml"));
		conf.addResource(new Path("/usr/local/hadoop-2.9.2/etc/hadoop/mapred-site.xml"));
		FileSystem hdfs = FileSystem.get(conf);
		//내컴퓨터의 파일 읽기
		String file ="src/main/java/hdfs/DisplayHadoopFile.java";
		FileInputStream fis = new FileInputStream(file);
		int len =0;
		byte[] buf = new byte[8096];
		//하둡서버에 저장할 파일 지정, path 도 같이 설정됨
		Path path = new Path(file);
		//out :하둡서버에 출력될 파일 지정
		FSDataOutputStream out = hdfs.create(path);
		while((len = fis.read(buf)) != -1) {
			out.writeUTF(new String(buf,0,len));
		}
		fis.close();out.flush();out.close();
	}

}
