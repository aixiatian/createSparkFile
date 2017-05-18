package is.sina.tool;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class GenShAndConf {

	
	
	public String getSql(String filename){
		File file = new File(filename);
		BufferedReader br = null;
		try {
			InputStreamReader fr = new InputStreamReader(new FileInputStream(file), "UTF-8");
			br = new BufferedReader(fr);
			StringBuffer sb = new StringBuffer();
			String line = null;
			while((line = br.readLine()) != null){
				sb.append(line+"\n");
			}
			String str = sb.toString();
			return str;
		} catch (Exception e) {
			e.printStackTrace();
		} finally{
			try {
				if(br != null)
					br.close();
			} catch (Exception e2) {
			}
		}
		return null;
	}
	
	public String getYmlSql(String filename){
		String basespace = "        ";
		File file = new File(filename);
		BufferedReader br = null;
		try {
			InputStreamReader fr = new InputStreamReader(new FileInputStream(file), "UTF-8");
			br = new BufferedReader(fr);
			StringBuffer sb = new StringBuffer();
			String line = null;
			while((line = br.readLine()) != null){
				String tmp = line.replace("\t", "    ");
				String tmp1 = tmp.toLowerCase();
				if(tmp1.startsWith("step-") || tmp1.startsWith("app_name:")){
					tmp = tmp.trim();
					sb.append(tmp+"\n");
				}else if(tmp.trim().equals("-")){
					sb.append("      "+tmp+"\n");
				}else{
					sb.append(basespace+tmp+"\n");
				}
			}
			String str = sb.toString();
			return str;
		} catch (Exception e) {
			e.printStackTrace();
		} finally{
			try {
				if(br != null)
					br.close();
			} catch (Exception e2) {
			}
		}
		return null;
	
	}
	
	public void createYml(String path, String outname, String str) {

		File file = new File(path + outname + ".yml");
		OutputStreamWriter out = null;
		try {

			out = new OutputStreamWriter(
					new FileOutputStream(file), "UTF-8");
			out.write(str.toCharArray());
			out.flush();
			System.out.println("success!");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (out != null)
					out.close();
			} catch (Exception e2) {
			}
		}

	}
	public void createFile(String type,String path,Map<String,String> param){
		createFile("template",type, path, param);
	}
	
	public void createSparkFile(String type,String path,Map<String,String> param){
		createFile("sparktemplate",type, path, param);
	}
	
	public void createFile(String templatname,String type,String path,Map<String,String> param){
		File file = new File("D:/createsh/"+templatname+"."+type);
		BufferedReader br = null;
		try {
			InputStreamReader fr = new InputStreamReader(new FileInputStream(file), "UTF-8");
			br = new BufferedReader(fr);
			StringBuffer sb = new StringBuffer();
			String line = null;
			while((line = br.readLine()) != null){
				sb.append(line+"\n");
			}
			String str = sb.toString();
			if("".equals(str = replaceAllPara(str, param)))
				return;
			if(param.containsKey("shellname")){
				String ofile = path+param.get("shellname")+"."+type;
				System.out.println(ofile);
				OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(ofile),"UTF-8");  
				out.write(str.toCharArray());  
				out.flush();  
				out.close();  
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally{
			try {
				if(br != null)
					br.close();
			} catch (Exception e2) {
			}
		}
	}
	
	public String replaceAllPara(String str,Map<String,String> param){
		if(str != null){
			for (String key : param.keySet()) {
				String reqkey = "@#@"+key+"@#@";
				str = str.replaceAll(reqkey, param.get(key));
			}
			str = str.replaceAll("`@`", "\\$");
		}
		return str;
	}
	
	public void getSparkFiles(String path,String inoutname){
//		String sql = getSql(path+inoutname+".sql");
//		sql = sql.replaceAll("\\$", "`@`");
//		System.out.println(sql);
//		String portal = "videoportal";
//		String logfile_start_hdfs = "/dw/ods/ods_mbportal_suda";
		String logfile_start_hdfs = "";
		String spark_mds_table = "spark_temp_output_column9";
//		String portal = "mbportal";
		String portal = "videoportal";
		String user = "dongkai3";
		String desc = "APP-点播人均播放时长";
		String job_id = "605013";
		String tablename = "rpt_sheet_mobileportal_spark_test_data";
		String filename = "video_play_per_duration";
		String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
		String job_name = "rpt_kpi_mobileportal_"+job_id;
		String confname = "spark_"+job_name;
//		String confname = "rpt_kpi_videoportal_"+job_id;
//		String shellname = confname;
//		String conf = "rpt/mbportal/client/push/";
//		String conf = "rpt/videoportal/videoplay/";
		String conf = "rpt/videoportal/video_app/";
//		String conf = "rpt/mbportal/wap/basic/";
//		String conf = "rpt/mbportal/client/sports/";
		String rkdate = "date";
		
		String ymlname = "spark_"+job_id+"_"+filename;
		
		if(portal.equals("videoportal"))
			rkdate = "hive_date";
		Map<String,String> param = new HashMap<String,String>();
		param.put("portal", portal);
		param.put("user", user);
		param.put("desc", desc);
		param.put("job_id", job_id);
		param.put("tablename", tablename);
		param.put("filename", filename);
		param.put("date", date);
		param.put("confname", ymlname);
		param.put("shellname", ymlname);
//		param.put("sql", sql);
		param.put("conf", conf);
		param.put("rkdate", rkdate);
		param.put("logfile_start_hdfs", logfile_start_hdfs);
		param.put("spark_mds_table", spark_mds_table);
		param.put("job_name", confname);
		param.put("type", ymlname);
		param.put("ymlname", ymlname);
		
		createSparkFile("sh", path, param);
		createSparkFile("conf", path, param);
		
	}
	
	public void getYml(String path ,String inname,String outname){
		String sql = getYmlSql(path+inname+".sql");
		createYml(path, outname, sql);
	}
	
	public static void main(String[] args) {
		GenShAndConf gsc = new GenShAndConf();
		String path = "E:/work/2017/05/18/";
		String inname = "spark_605013_video_play_per_duration";
		String outname = "spark_605013_video_play_per_duration";
//		gsc.genJobSh();
//		gsc.getYml("other_refdomain", "other_refdomain");
//		gsc.getYml("referwm_detail_other", "referwm_detail_other");
		gsc.getYml(path,inname, outname);
		gsc.getSparkFiles(path, inname);
	}

	public void genJobSh(){
		
		String path = "E:/work/2017/05/12/";
		String sql = getSql(path+"template.sql");
		sql = sql.replaceAll("\\$", "`@`");
		System.out.println(sql);
//		String portal = "videoportal";
		String portal = "mbportal";
		String user = "dongkai3";
		String desc = "汽车事业部重点栏目-文章类-UV/PV/时长";
		String job_id = "715003";
		String tablename = "rpt_sheet_mobileportal_wap_auto_imp";
		String filename = "auto_important_column";
		String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
		String confname = "rpt_kpi_mobileportal_"+job_id;
//		String confname = "rpt_kpi_videoportal_"+job_id;
		String shellname = confname;
//		String conf = "rpt/mbportal/client/push/config/";
//		String conf = "rpt/videoportal/videoplay/config/";
		String conf = "rpt/mbportal/wap/basic/config/";
//		String conf = "rpt/mbportal/client/sports/config/";
		String rkdate = "date";
		if(portal.equals("videoportal"))
			rkdate = "hive_date";
		Map<String,String> param = new HashMap<String,String>();
		param.put("portal", portal);
		param.put("user", user);
		param.put("desc", desc);
		param.put("job_id", job_id);
		param.put("tablename", tablename);
		param.put("filename", filename);
		param.put("date", date);
		param.put("confname", confname);
		param.put("shellname", shellname);
		param.put("sql", sql);
		param.put("conf", conf);
		param.put("rkdate", rkdate);
		
		createFile("sh", path, param);
		createFile("conf", path, param);
	}
}
