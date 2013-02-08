package jp.projects.miya.drools_builder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

import jp.projects.miya.drools_hadoop.FactData;

public class DrlMaker {
	private static final int SEPARATE_SIZE = 10000;
	
	public static void main(String[] args) throws Exception {
		
		String targetDir = args[0] + System.getProperty("file.separator");
		File outDir = new File(targetDir);
		if (outDir.exists() != true) {
			outDir.mkdirs();
		}

		int ruleCount = Integer.parseInt(args[1]);
		for (int i = 0; i < ruleCount; i++) {

			File file = new File(targetDir + System.getProperty("file.separator") + args[1] + ".drl");

			
			FileOutputStream fos = new FileOutputStream(file);
			OutputStreamWriter osw = new OutputStreamWriter(fos);
			PrintWriter pw = new PrintWriter(osw);

			pw.println(FactData.class.getPackage() + ";");
			pw.println("");
			pw.println("import " + FactData.class.getCanonicalName() + ";");
			pw.println("");
			pw.println("dialect \"mvel\"");
			pw.println("");
			
			for (int j = i * SEPARATE_SIZE + 1; j <= (i + 1) * SEPARATE_SIZE && j <= ruleCount; j++) {
				pw.println("rule \"rule-" + j + "\"");
				pw.println("	when");
				pw.println("		d : FactData( d.type == \"xY\" )");
				pw.println("	then");
				pw.println("		d.value = \"width:\" + d.width + \" < height:\" + d.height;");
				pw.println("end");
			}

			pw.close();
			osw.close();
			fos.close();
		}
	}
}
