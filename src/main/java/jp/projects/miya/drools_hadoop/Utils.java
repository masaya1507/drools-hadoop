package jp.projects.miya.drools_hadoop;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.drools.compiler.PackageBuilderConfiguration;
import org.drools.rule.builder.dialect.java.JavaDialectConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {
	public static final Logger LOG = LoggerFactory.getLogger(Utils.class);
	
	public static ArrayList<File> getFileList(String[] list) {
		ArrayList<File> fileList = new ArrayList<File>();
		for (String file : list) {
			File f = new File(file);
			
			if (f.exists() != true) {
				continue;
			}
			if (f.isDirectory()) {
				ArrayList<String> inFiles = new ArrayList<String>();
				for (String name : f.list()) {
					inFiles.add(f.getPath() + System.getProperty("file.separator") + name);
				}
				fileList.addAll(Utils.getFileList(inFiles.toArray(new String[0])));
			} else {
				fileList.add(f);
			}
		}
		return fileList;
	}
	

	/**
	 * 
	 * @param quoted
	 * @return
	 */
	public static String trimQuote(String quoted) {
		if (quoted == null) {
			return null;
		} else if (quoted.matches("\".+\"$")) {
			return quoted.substring(1, quoted.length() - 1).trim();
		} else {
			return quoted;
		}
	}
	
	public static InputStream getDistributedCacheInputStream(Configuration conf, Path specifiedPath) throws IOException {
		FileSystem fs = LocalFileSystem.getLocal(conf);
		Path pathTarget = specifiedPath;
		if (fs.exists(pathTarget) != true) {
			Utils.LOG.info("DistributedCache Not Exists");
			File tagetFile = new File(specifiedPath.toString());
			ArrayList<File> fileChildList = Utils.getFileList(new String[] { tagetFile.getParent() });
			for (File child : fileChildList) {
				Utils.LOG.info("DistributedCache CurrentDir childs : " + child.getAbsolutePath());
				if (child.getName().equals(specifiedPath.getName())) {
					Utils.LOG.info("DistributedCache found.");
					return fs.open(new Path(child.toURI()));
				}
			}
			throw new IOException("DistributedCache Not Exists (and childs)");
		} else {
			Utils.LOG.info("DistributedCache Exists");
			return fs.open(specifiedPath);
		}
	}
	
	public static PackageBuilderConfiguration getDroolsConf() {
		Properties properties = new Properties();
		properties.setProperty("drools.dialect.java.compiler", "JANINO");
		PackageBuilderConfiguration cfg =  new PackageBuilderConfiguration(properties);
		JavaDialectConfiguration javaConf = 
				(JavaDialectConfiguration) cfg.getDialectConfiguration("java");
		javaConf.setCompiler(JavaDialectConfiguration.JANINO);
		
		return cfg;
	}
}
