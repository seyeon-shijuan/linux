package hdfs;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

//키보드에서 데이터를 입력해 하둡파일로 저장
public class WriteHadoopFile {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/hadoop-2.9.2/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/hadoop-2.9.2/etc/hadoop/hdfs-site.xml"));
		conf.addResource(new Path("/usr/local/hadoop-2.9.2/etc/hadoop/mapred-site.xml"));
		FileSystem hdfs = FileSystem.get(conf);
		Path path = new Path("consoledata");
		System.out.println("하둡 파일에 저장할 내용을 입력하세요.");
		Scanner scan = new Scanner(System.in);
		//hdfs.create(path) :하둡 서버에 path의 파일을 생성하기
		FSDataOutputStream out = hdfs.create(path);
		while(true) {
			String console = scan.nextLine();
			if(console.equals("exit")) break;
			out.writeUTF(console+"\n");
		}
		out.flush();out.close();
	}

}
