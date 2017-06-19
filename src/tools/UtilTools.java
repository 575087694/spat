package tools;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class UtilTools {
	
	// 计算2个时间的差值
	public static long compTimeDiff(String starttime, String endtime) {
		if (starttime.length() <= 0 || endtime.length() <= 0) {
			return 0;
		}
		long diff = -1;
		long time = -1;
		try {
			time = new Long(endtime.substring(23, 26)) - new Long(starttime.substring(23, 26));
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

			Date begin = df.parse(starttime.substring(0, 23));
			Date end = df.parse(endtime.substring(0, 23));
			diff = end.getTime() - begin.getTime();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return diff * 1000 + time;
	}

	// 扫描指定目录下的所有文件
	public static void getFiles(ArrayList<String> filelist, ArrayList<String> subdirlist, String filePath,
			String fileKey) {
		File root = new File(filePath);
		File[] files = root.listFiles();
		for (File file : files) {
			if (file.isDirectory()) {
				getFiles(filelist, subdirlist, file.getAbsolutePath(), fileKey);
				subdirlist.add(file.getAbsolutePath().substring(file.getAbsolutePath().lastIndexOf("\\") + 1));
			} else {
				String strFileName = file.getAbsolutePath();
				if (strFileName.indexOf(fileKey) >= 0)
					filelist.add(strFileName);
			}
		}
	}
	
	// 创建数据库连接
	public static Connection createConnection(String dbname, boolean type, String path) {
		Connection conn = null;
		String JDBC_URL = "";
		String USER = "mclogger";
		String PASSWORD = "123456";
		String DRIVER_CLASS = "org.h2.Driver";
		try {
			if (dbname == null || dbname.trim().length() <= 0) {
				dbname = "test";
			}
			if (path == null || path.trim().length() <= 0) {
				path = ".";
			}
			if (type == true) {
				JDBC_URL = "jdbc:h2:mem:" + dbname;
			} else {
				JDBC_URL = "jdbc:h2:file:" + path + "\\" + dbname;
			}
			Class.forName(DRIVER_CLASS);
			conn = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return conn;
	}
	
}
