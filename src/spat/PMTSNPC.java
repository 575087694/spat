package spat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import javax.swing.JTextArea;

import tools.Data;
import tools.UtilTools;

public class PMTSNPC implements Data{
	int count = 0; // SQL����
	String msgblock = new String(); // ��־��
	String filepath = new String(); // ��־Ŀ¼
	String filter = new String(); // ��־�ؼ���
	Connection dbconn; // ���ݿ����ӳ�
	PreparedStatement ps; // Ԥ����SQL
	JTextArea textArea = null; // �����
	PMTSNPCMAP pmtsnpcmap = new PMTSNPCMAP(); // ��־�������

	class PMTSNPCMAP {
		String msgtype;// ��������
		String pid;// ���̺�
		String msgdirection; // ���Ĵ��䷽��
		String msgsendtime; // ���ĵķ���ʱ��
		String premsgsendtime; // ǰһ�ڵ㷢��ʱ��
		String pmtsurecvtime; // NPC��¼�Ľ���ʱ��(Uͷ)
		long msgfwtime; // NPC����ת��ʱ��
		long msgdealtime; // NPCӦ�ô���ʱ��
	}

	// ��ʼ������
	public PMTSNPC(Connection dbconn, String filepath, String filter, JTextArea textArea) {
		this.dbconn = dbconn;
		this.filepath = filepath;
		this.filter = filter;
		this.textArea = textArea;
	}

	// ��ʽ�����Ȳ���΢���ʱ�䴮
	public String formatTime(String time) {
		int len = time.length();
		for (int i = 0; i < 26 - len; i++) {
			time = time + '0';
		}
		return time;
	}

	// ��ȡ���̺�
	public void getPid() {
		int pos1 = msgblock.indexOf("[", 2);
		int pos2 = msgblock.indexOf("]", pos1);
		if (pos1 != -1 && pos2 != -1) {
			pmtsnpcmap.pid = msgblock.substring(pos1 + 1, pos2);
		} else {
			pmtsnpcmap.pid = "";
		}
	}

	// ��ȡ���ķ���ʱ��
	public void getMsgSendTime() {
		int pos = msgblock.indexOf("[");
		if (pos != -1) {
			pmtsnpcmap.msgsendtime = msgblock.substring(pos + 1, pos + 27);
		} else {
			pmtsnpcmap.msgsendtime = "";
		}
	}

	// ��ȡǰһ�ڵ㷢��ʱ��
	public void getPreMsgSendTime() {
		int pos = msgblock.indexOf(PMTSPRENODETIMEFLAG);
		int pos1 = msgblock.indexOf("[", pos);
		int pos2 = msgblock.indexOf("]", pos1);
		if (pos != -1 && pos1 != -1 && pos2 != -1) {
			pmtsnpcmap.premsgsendtime = msgblock.substring(pos1 + 1, pos2);
		} else {
			pmtsnpcmap.premsgsendtime = "";
		}
	}

	// ��ȡ�������ͺͱ��Ĵ��䷽��
	public void getMsgType() {
		try {
			int pos = -1;
			if ((pos = msgblock.indexOf(IBPSMSGHEAD)) != -1) {
				pmtsnpcmap.msgtype = msgblock.substring(pos + 54, pos + 74);
				pmtsnpcmap.msgdirection = msgblock.substring(pos + 115, pos + 116);
			} else if ((pos = msgblock.indexOf(CNAPSMSGHEAD)) != 1) {
				pmtsnpcmap.msgtype = msgblock.substring(pos + 58, pos + 78);
				pmtsnpcmap.msgdirection = msgblock.substring(pos + 119, pos + 120);
			} else {
				throw new Exception("����ͷ������������ͷ���������ͷ��\n");
			}
			pmtsnpcmap.msgtype = pmtsnpcmap.msgtype.trim();

		} catch (Exception e) {
			textArea.append(e.getMessage() + "\n");
		}

	}

	public void getPmtsURecvTime() {
		int pos = -1;
		if ((pos = msgblock.indexOf(PMTSUHEADFLAG)) != -1 && pmtsnpcmap.msgdirection.equals("D")) {
			pmtsnpcmap.pmtsurecvtime = msgblock.substring(pos + 417, pos + 440);
			pmtsnpcmap.pmtsurecvtime = pmtsnpcmap.msgsendtime.substring(0, 8)
					+ pmtsnpcmap.pmtsurecvtime.trim().replace('T', ' ');
		} else {
			pmtsnpcmap.pmtsurecvtime = "";
		}
	}

	public void getMsgDealTime() {
		if (pmtsnpcmap.msgdirection.equals("D")) {
			pmtsnpcmap.msgdealtime = UtilTools.compTimeDiff(formatTime(pmtsnpcmap.pmtsurecvtime), formatTime(pmtsnpcmap.msgsendtime));
			pmtsnpcmap.msgfwtime = 0;
		} else {
			pmtsnpcmap.msgfwtime = UtilTools.compTimeDiff(formatTime(pmtsnpcmap.premsgsendtime), formatTime(pmtsnpcmap.msgsendtime));
			pmtsnpcmap.msgdealtime = 0;
		}
	}

	// ��ʼ����
	public void initPMTSNPCTab(Connection conn) {
		try {
			Statement stmt = conn.createStatement();
			stmt.executeUpdate("DROP TABLE IF EXISTS TABPMTSNPC");
			stmt.executeUpdate(
					"CREATE TABLE TABPMTSNPC(MSGTYPE CHAR(15),PID CHAR(8),MSGDIRECTION CHAR(1),MSGSENDTIME CHAR(26),PREMSGSENDTIME CHAR(26),PMTSURECVTIME CHAR(26),MSGDEALTIME INT,MSGFWTIME INT);");
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	// ����������ݿ�
	public void insertDataBase() {
		try {
			if (count == 0) {
				String sql = "INSERT INTO TABPMTSNPC VALUES(?,?,?,?,?,?,?,?)";
				ps = dbconn.prepareStatement(sql);
			}
			ps.setString(1, pmtsnpcmap.msgtype);
			ps.setString(2, pmtsnpcmap.pid);
			ps.setString(3, pmtsnpcmap.msgdirection);
			ps.setString(4, pmtsnpcmap.msgsendtime);
			ps.setString(5, pmtsnpcmap.premsgsendtime);
			ps.setString(6, pmtsnpcmap.pmtsurecvtime);
			ps.setLong(7, pmtsnpcmap.msgdealtime);
			ps.setLong(8, pmtsnpcmap.msgfwtime);
			ps.addBatch();
			++count;
			if (count == 1000) {
				ps.executeBatch();
				ps.close();
				count = 0;
			}
		} catch (SQLException e) {
			textArea.append("���ݲ������ݿ�ʧ�ܣ�\n\n");
		}
	}

	// ������־��
	public void parseMCTL() {
		getPid();
		getMsgSendTime();
		getPreMsgSendTime();
		getMsgType();
		getPmtsURecvTime();
		if ((pmtsnpcmap.pmtsurecvtime.isEmpty() && pmtsnpcmap.msgdirection.equals("D"))
				|| (pmtsnpcmap.premsgsendtime.isEmpty() && pmtsnpcmap.msgdirection.equals("U"))) {
			return;
		}
		getMsgDealTime();
		insertDataBase();
	}

	// ������
	public void runMainMethod() {
		ArrayList<String> filelist = new ArrayList<String>();
		ArrayList<String> sdirlist = new ArrayList<String>();
		UtilTools.getFiles(filelist, sdirlist, filepath, filter);
		initPMTSNPCTab(dbconn);
		try {
			boolean sign = false;
			String line = new String();
			for (String path : filelist) {
				File f = new File(path);
				FileInputStream fi = new FileInputStream(f);
				InputStreamReader in = new InputStreamReader(fi, "UTF-8");
				BufferedReader br = new BufferedReader(in);
				while ((line = br.readLine()) != null) {
					if (sign) {
						if (line.indexOf(MSGBODYFINSHEDFLAG) != -1) {
							msgblock = msgblock + line;
							parseMCTL();
							sign = false;
						} else if (line.indexOf(PMTSMSGSTARTFLAG) != -1) {
							msgblock = line;
						} else {
							msgblock = msgblock + line;
						}
					} else {
						if (line.indexOf(PMTSMSGSTARTFLAG) != -1) {
							sign = true;
							msgblock = line;
						}
					}
				}
				br.close();
				in.close();
				fi.close();
			}
			if (count != 0) {
				ps.executeBatch();
				ps.close();
				count = 0;
			}
		} catch (SQLException e) {
			textArea.append("���ݿ�����ʧ�ܣ�\n\n");
		} catch (FileNotFoundException e) {
			textArea.append("�ļ�δ�ҵ���\n\n");
		} catch (IOException e) {
			textArea.append("�ļ���ȡ����\n\n");
		} catch (Exception e) {
			textArea.append("��������\n\n");
			e.printStackTrace();
		}
	}
}
