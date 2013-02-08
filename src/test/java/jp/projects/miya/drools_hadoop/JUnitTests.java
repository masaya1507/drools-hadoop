package jp.projects.miya.drools_hadoop;

import jp.projects.miya.drools_builder.DrlMaker;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

/**
 * Unit test for MapReduceDriver.
 */
public class JUnitTests
{
	@Test
	public void testMapReduceJob() throws Exception {
		String drlDir = "/Users/mbp_user/devel/java/drools-hadoop/drl/";
		String hdfsOut = "output";

		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(new Path(hdfsOut), true);

		MapReduceDriver.main(new String[] {
				"-files",
				drlDir + "rule.drl",
				"input",
				"output"
				}
		);
	}

	@Test
	public void testDrlWriter() throws Exception {
		String drlDir = "/Users/mbp_user/devel/java/drools-hadoop/drl/";
		DrlMaker.main(new String[] {drlDir, "100"});
	}
}
