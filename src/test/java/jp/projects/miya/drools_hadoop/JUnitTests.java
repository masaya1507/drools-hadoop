package jp.projects.miya.drools_hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

/**
 * Unit test for MapReduceDriver.
 */
public class JUnitTests {
	@Test
	public void testMapReduceJob1() throws Exception {
		String drlDir = "/Users/mbp_user/devel/java/drools-hadoop/drl/";
		String hdfsOut = "output";

		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(new Path(hdfsOut), true);

		MapReduceByStateless.main(new String[] {
				"-files",
				drlDir + "rule1.drl",
				"input",
				"output"
				}
		);
	}
	
	@Test
	public void testMapReduceJob2() throws Exception {
		String drlDir = "/Users/mbp_user/devel/java/drools-hadoop/drl/";
		String hdfsOut = "output";

		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(new Path(hdfsOut), true);

		MapReduceByStateful.main(new String[] {
				"-files",
				drlDir + "rule2.drl",
				"input",
				"output"
				}
		);
	}

	@Test
	public void testMapReduceJob3() throws Exception {
		String drlDir = "/Users/mbp_user/devel/java/drools-hadoop/drl/";
		String hdfsOut = "output";

		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(new Path(hdfsOut), true);

		MapReduceByStatefulWithDetail.main(new String[] {
				"-files",
				drlDir + "rule2.drl",
				"input",
				"output"
				}
		);
	}
	
	@Test
	public void testMapReduceExternal() throws Exception {
		String drlDir = "/Users/mbp_user/devel/java/drools-hadoop/drl/";
		String hdfsOut = "output";

		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(new Path(hdfsOut), true);
		
		MapReduceByStateful.main(new String[] {
				"-D", "mapred.job.tracker=192.168.3.8:8021",
				"-D", "fs.default.name=hdfs://192.168.3.8:8020",
				"-libjars", "/Users/mbp_user/devel/java/drools-hadoop/target/sample-0.0.1-SNAPSHOT.jar,/Users/mbp_user/.m2/repository/ant/ant/1.6.5/ant-1.6.5.jar,/Users/mbp_user/.m2/repository/antlr/antlr/2.7.7/antlr-2.7.7.jar,/Users/mbp_user/.m2/repository/asm/asm/3.2/asm-3.2.jar,/Users/mbp_user/.m2/repository/com/cloudera/cdh/hadoop-ant/0.20.2-cdh3u5/hadoop-ant-0.20.2-cdh3u5.pom,/Users/mbp_user/.m2/repository/com/sun/jersey/jersey-core/1.8/jersey-core-1.8.jar,/Users/mbp_user/.m2/repository/com/sun/jersey/jersey-json/1.8/jersey-json-1.8.jar,/Users/mbp_user/.m2/repository/com/sun/jersey/jersey-server/1.8/jersey-server-1.8.jar,/Users/mbp_user/.m2/repository/com/sun/xml/bind/jaxb-impl/2.2.3-1/jaxb-impl-2.2.3-1.jar,/Users/mbp_user/.m2/repository/com/thoughtworks/xstream/xstream/1.4.1/xstream-1.4.1.jar,/Users/mbp_user/.m2/repository/commons-cli/commons-cli/1.2/commons-cli-1.2.jar,/Users/mbp_user/.m2/repository/commons-codec/commons-codec/1.4/commons-codec-1.4.jar,/Users/mbp_user/.m2/repository/commons-el/commons-el/1.0/commons-el-1.0.jar,/Users/mbp_user/.m2/repository/commons-httpclient/commons-httpclient/3.1/commons-httpclient-3.1.jar,/Users/mbp_user/.m2/repository/commons-io/commons-io/2.1/commons-io-2.1.jar,/Users/mbp_user/.m2/repository/commons-logging/commons-logging/1.1.1/commons-logging-1.1.1.jar,/Users/mbp_user/.m2/repository/commons-net/commons-net/1.4.1/commons-net-1.4.1.jar,/Users/mbp_user/.m2/repository/hsqldb/hsqldb/1.8.0.7/hsqldb-1.8.0.7.jar,/Users/mbp_user/.m2/repository/javax/activation/activation/1.1/activation-1.1.jar,/Users/mbp_user/.m2/repository/javax/servlet/jsp-api/2.0/jsp-api-2.0.jar,/Users/mbp_user/.m2/repository/javax/servlet/servlet-api/2.5/servlet-api-2.5.jar,/Users/mbp_user/.m2/repository/javax/servlet/jsp/jsp-api/2.1/jsp-api-2.1.jar,/Users/mbp_user/.m2/repository/javax/xml/bind/jaxb-api/2.2.2/jaxb-api-2.2.2.jar,/Users/mbp_user/.m2/repository/javax/xml/stream/stax-api/1.0-2/stax-api-1.0-2.jar,/Users/mbp_user/.m2/repository/junit/junit/4.10/junit-4.10.jar,/Users/mbp_user/.m2/repository/log4j/log4j/1.2.15/log4j-1.2.15.jar,/Users/mbp_user/.m2/repository/net/java/dev/jets3t/jets3t/0.6.1/jets3t-0.6.1.jar,/Users/mbp_user/.m2/repository/net/sf/opencsv/opencsv/2.3/opencsv-2.3.jar,/Users/mbp_user/.m2/repository/org/antlr/antlr/3.3/antlr-3.3.jar,/Users/mbp_user/.m2/repository/org/antlr/antlr-runtime/3.3/antlr-runtime-3.3.jar,/Users/mbp_user/.m2/repository/org/antlr/stringtemplate/3.2.1/stringtemplate-3.2.1.jar,/Users/mbp_user/.m2/repository/org/apache/hadoop/hadoop-core/0.20.2-cdh3u5/hadoop-core-0.20.2-cdh3u5.jar,/Users/mbp_user/.m2/repository/org/apache/hadoop/thirdparty/guava/guava/r09-jarjar/guava-r09-jarjar.jar,/Users/mbp_user/.m2/repository/org/apache/mrunit/mrunit/0.9.0-incubating/mrunit-0.9.0-incubating-hadoop1.jar,/Users/mbp_user/.m2/repository/org/codehaus/jackson/jackson-core-asl/1.5.2/jackson-core-asl-1.5.2.jar,/Users/mbp_user/.m2/repository/org/codehaus/jackson/jackson-jaxrs/1.7.1/jackson-jaxrs-1.7.1.jar,/Users/mbp_user/.m2/repository/org/codehaus/jackson/jackson-mapper-asl/1.5.2/jackson-mapper-asl-1.5.2.jar,/Users/mbp_user/.m2/repository/org/codehaus/jackson/jackson-xc/1.7.1/jackson-xc-1.7.1.jar,/Users/mbp_user/.m2/repository/org/codehaus/janino/janino/2.5.16/janino-2.5.16.jar,/Users/mbp_user/.m2/repository/org/codehaus/jettison/jettison/1.1/jettison-1.1.jar,/Users/mbp_user/.m2/repository/org/drools/drools-compiler/5.5.0.Final/drools-compiler-5.5.0.Final.jar,/Users/mbp_user/.m2/repository/org/drools/drools-core/5.5.0.Final/drools-core-5.5.0.Final.jar,/Users/mbp_user/.m2/repository/org/drools/knowledge-api/5.5.0.Final/knowledge-api-5.5.0.Final.jar,/Users/mbp_user/.m2/repository/org/drools/knowledge-internal-api/5.5.0.Final/knowledge-internal-api-5.5.0.Final.jar,/Users/mbp_user/.m2/repository/org/eclipse/jdt/core/3.1.1/core-3.1.1.jar,/Users/mbp_user/.m2/repository/org/eclipse/jdt/core/compiler/ecj/3.5.1/ecj-3.5.1.jar,/Users/mbp_user/.m2/repository/org/hamcrest/hamcrest-core/1.1/hamcrest-core-1.1.jar,/Users/mbp_user/.m2/repository/org/mockito/mockito-all/1.8.5/mockito-all-1.8.5.jar,/Users/mbp_user/.m2/repository/org/mortbay/jetty/jetty/6.1.26/jetty-6.1.26.jar,/Users/mbp_user/.m2/repository/org/mortbay/jetty/jetty-util/6.1.26/jetty-util-6.1.26.jar,/Users/mbp_user/.m2/repository/org/mortbay/jetty/servlet-api/2.5-20081211/servlet-api-2.5-20081211.jar,/Users/mbp_user/.m2/repository/org/mvel/mvel2/2.1.3.Final/mvel2-2.1.3.Final.jar,/Users/mbp_user/.m2/repository/org/slf4j/slf4j-api/1.6.6/slf4j-api-1.6.6.jar,/Users/mbp_user/.m2/repository/org/slf4j/slf4j-log4j12/1.6.6/slf4j-log4j12-1.6.6.jar,/Users/mbp_user/.m2/repository/oro/oro/2.0.8/oro-2.0.8.jar,/Users/mbp_user/.m2/repository/stax/stax-api/1.0.1/stax-api-1.0.1.jar,/Users/mbp_user/.m2/repository/tomcat/jasper-compiler/5.5.23/jasper-compiler-5.5.23.jar,/Users/mbp_user/.m2/repository/tomcat/jasper-runtime/5.5.23/jasper-runtime-5.5.23.jar,/Users/mbp_user/.m2/repository/xmlenc/xmlenc/0.52/xmlenc-0.52.jar,/Users/mbp_user/.m2/repository/xmlpull/xmlpull/1.1.3.1/xmlpull-1.1.3.1.jar,/Users/mbp_user/.m2/repository/xpp3/xpp3_min/1.1.4c/xpp3_min-1.1.4c.jar",
				"-files",
				drlDir + "rule2.drl",
				"/user/root/",
				"/tmp/output"
				}
		);
	}
}
