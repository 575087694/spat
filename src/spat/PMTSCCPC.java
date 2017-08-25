package spat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import javax.swing.JTextArea;

import tools.Data;
import tools.UtilTools;

public class PMTSCCPC implements Data{
	int count = 0; // SQL计数
	StringBuilder msgblock = new StringBuilder(); // 日志块
	String filepath = new String(); // 日志目录
	String filter = new String(); // 日志关键字
	Connection dbconn; // 数据库连接池
	PreparedStatement ps; // 预处理SQL
	JTextArea textArea = null; // 输出框
	PMTSCCPCMAP pmtsccpcmap = new PMTSCCPCMAP(); // 日志分析结果

	class PMTSCCPCMAP {
		String msgtype;// 报文类型
		String pid;// 进程号
		String msgdirection; // 报文传输方向
		String msgsendtime; // 报文的发送时间
		String premsgsendtime; // 前一节点发送时间
		String pmtsurecvtime; // NPC记录的接收时间(U头)
		long msgfwtime; // NPC报文转发时间
		long msgdealtime; // NPC应用处理时间
	}

	// 初始化参数
	public PMTSCCPC(Connection dbconn, String filepath, String filter, JTextArea textArea) {
		this.dbconn = dbconn;
		this.filepath = filepath;
		this.filter = filter;
		this.textArea = textArea;
	}

	// 格式化精度不到微秒的时间串
	public static String formatTime(String time) {
		int len = time.length();
		for (int i = 0; i < 26 - len; i++) {
			time = time + '0';
		}
		return time;
	}

	// 获取进程号
	public void getPid() {
		int pos1 = msgblock.indexOf("[", 2);
		int pos2 = msgblock.indexOf("]", pos1);
		if (pos1 != -1 && pos2 != -1) {
			pmtsccpcmap.pid = msgblock.substring(pos1 + 1, pos2);
		} else {
			pmtsccpcmap.pid = "";
		}
	}

	// 获取报文发送时间
	public void getMsgSendTime() {
		int pos = msgblock.indexOf("[");
		if (pos != -1) {
			pmtsccpcmap.msgsendtime = msgblock.substring(pos + 1, pos + 27);
		} else {
			pmtsccpcmap.msgsendtime = "";
		}
	}

	// 获取前一节点发送时间
	public void getPreMsgSendTime() {
		int pos = msgblock.indexOf(PMTSPRENODETIMEFLAG);
		int pos1 = msgblock.indexOf("[", pos);
		int pos2 = msgblock.indexOf("]", pos1);
		if (pos != -1 && pos1 != -1 && pos2 != -1) {
			pmtsccpcmap.premsgsendtime = msgblock.substring(pos1 + 1, pos2);
		} else {
			pmtsccpcmap.premsgsendtime = "";
		}
	}

	// 获取报文类型和报文传输方向
	public void getMsgType() {
		try {
			int pos = -1;
			if ((pos = msgblock.indexOf(IBPSMSGHEAD)) != -1) {
				pmtsccpcmap.msgtype = msgblock.substring(pos + 54, pos + 74);
				pmtsccpcmap.msgdirection = msgblock.substring(pos + 115, pos + 116);
			} else if ((pos = msgblock.indexOf(CNAPSMSGHEAD)) != 1) {
				pmtsccpcmap.msgtype = msgblock.substring(pos + 58, pos + 78);
				pmtsccpcmap.msgdirection = msgblock.substring(pos + 119, pos + 120);
			} else {
				throw new Exception("报文头不是网银报文头或二代报文头！\n");
			}
			pmtsccpcmap.msgtype = pmtsccpcmap.msgtype.trim();

		} catch (Exception e) {
			textArea.append(e.getMessage() + "\n");
		}

	}

	public void getPmtsURecvTime() {
		int pos = -1;
		if ((pos = msgblock.indexOf(PMTSUHEADFLAG)) != -1 && pmtsccpcmap.msgdirection.equals("D")) {
			pmtsccpcmap.pmtsurecvtime = msgblock.substring(pos + 288, pos + 288 + 23);
			pmtsccpcmap.pmtsurecvtime = pmtsccpcmap.msgsendtime.substring(0, 8)
					+ pmtsccpcmap.pmtsurecvtime.trim().replace('T', ' ');
		} else {
			pmtsccpcmap.pmtsurecvtime = "";
		}
	}

	public void getMsgDealTime() {
		if (pmtsccpcmap.msgdirection.equals("D")) {
			pmtsccpcmap.msgfwtime = UtilTools.compTimeDiff(formatTime(pmtsccpcmap.premsgsendtime), formatTime(pmtsccpcmap.msgsendtime));
			pmtsccpcmap.msgdealtime = 0;
		} else {
			pmtsccpcmap.msgdealtime = UtilTools.compTimeDiff(formatTime(pmtsccpcmap.pmtsurecvtime), formatTime(pmtsccpcmap.msgsendtime));
			pmtsccpcmap.msgfwtime = 0;
		}
	}

	// 初始化表
	public void initPMTSCCPCTab(Connection conn) {
		try {
			Statement stmt = conn.createStatement();
			stmt.executeUpdate("DROP TABLE IF EXISTS TABPMTSCCPC");
			stmt.executeUpdate(
					"CREATE TABLE TABPMTSCCPC(MSGTYPE CHAR(15),PID CHAR(8),MSGDIRECTION CHAR(1),MSGSENDTIME CHAR(26),PREMSGSENDTIME CHAR(26),PMTSURECVTIME CHAR(26),MSGDEALTIME INT,MSGFWTIME INT);");
			stmt.close();
		} catch (SQLException e) {
			textArea.append(e.getMessage()+"\n\n");
			e.printStackTrace();
		}
	}

	// 结果插入数据库
	public void insertDataBase() {
		try {
			if (count == 0) {
				String sql = "INSERT INTO TABPMTSCCPC VALUES(?,?,?,?,?,?,?,?)";
				ps = dbconn.prepareStatement(sql);
			}
			ps.setString(1, pmtsccpcmap.msgtype);
			ps.setString(2, pmtsccpcmap.pid);
			ps.setString(3, pmtsccpcmap.msgdirection);
			ps.setString(4, pmtsccpcmap.msgsendtime);
			ps.setString(5, pmtsccpcmap.premsgsendtime);
			ps.setString(6, pmtsccpcmap.pmtsurecvtime);
			ps.setLong(7, pmtsccpcmap.msgdealtime);
			ps.setLong(8, pmtsccpcmap.msgfwtime);
			ps.addBatch();
			++count;
			if (count == 1000) {
				ps.executeBatch();
				ps.close();
				count = 0;
			}
		} catch (SQLException e) {
			textArea.append(e.getMessage()+"\n\n");
			e.printStackTrace();
		}
	}

	// 解析日志块
	public void parsePMTS() {
		getPid();
		getMsgSendTime();
		getPreMsgSendTime();
		getMsgType();
		getPmtsURecvTime();
		if ((pmtsccpcmap.premsgsendtime.isEmpty() && pmtsccpcmap.msgdirection.equals("D"))
				|| (pmtsccpcmap.pmtsurecvtime.isEmpty() && pmtsccpcmap.msgdirection.equals("U"))) {
			return;
		}
		getMsgDealTime();
		insertDataBase();
	}

	// 主方法
	public void runMainMethod() {
		ArrayList<String> filelist = new ArrayList<String>();
		ArrayList<String> sdirlist = new ArrayList<String>();
		UtilTools.getFiles(filelist, sdirlist, filepath, filter);
		initPMTSCCPCTab(dbconn);
		try {
			boolean sign = false;
			for (String path : filelist) {
				List<String> lines = Files.readAllLines(Paths.get(path), StandardCharsets.UTF_8); 
				for(String line : lines){ 
					if (sign) {
						if (line.indexOf(MSGBODYFINSHEDFLAG) != -1) {
							msgblock.append(line);
							parsePMTS();
							sign = false;
						} else if (line.indexOf(PMTSMSGSTARTFLAG) != -1) {
							msgblock.setLength(0);
							msgblock.append(line);
						} else {
							msgblock.append(line);
						}
					} else {
						if (line.indexOf(PMTSMSGSTARTFLAG) != -1) {
							sign = true;
							msgblock.setLength(0);
							msgblock.append(line);
						}
					}
				}
			}
			if (count != 0) {
				ps.executeBatch();
				ps.close();
				count = 0;
			}
		} catch (Exception e) {
			textArea.append(e.getMessage()+"\n\n");
			e.printStackTrace();
		}
	}
}