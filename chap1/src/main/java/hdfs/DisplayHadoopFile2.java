package hdfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DisplayHadoopFile2 {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
			Configuration conf = new Configuration();
			conf.addResource(new Path("/usr/local/hadoop-2.9.2/etc/hadoop/core-site.xml"));
			conf.addResource(new Path("/usr/local/hadoop-2.9.2/etc/hadoop/hdfs-site.xml"));
			conf.addResource(new Path("/usr/local/hadoop-2.9.2/etc/hadoop/mapred-site.xml"));
			FileSystem fs = FileSystem.get(conf);
			Path pt = new Path("consoledata");
			FSDataInputStream br = fs.open(pt);

			while(true) {
				try {
				System.out.print( br.readUTF());
				} catch (Exception e) {
					break;
				}
		}
			br.close();
	}


}
