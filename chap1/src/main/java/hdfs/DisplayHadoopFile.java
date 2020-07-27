package hdfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DisplayHadoopFile {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			String filepath ="hdfs://localhost:9000/user/hadoop/in";
			Path pt = new Path(filepath);
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", filepath);
			FileSystem fs = FileSystem.get(conf);
			//fs.open(pt) : hadoop 서버에 연결하여 파일 읽기
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
			String line = null;
			while((line = br.readLine()) != null) {
				System.out.println(line);
			}
			br.close();
		}catch (IOException e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

}
