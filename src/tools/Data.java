package tools;

public interface Data {
	String MCTLSQL0 = "所有记录##select * from tabmctl";
	String MCTLSQL1 = "报文总数##select formatdatetime(MSGDEALSTIME, 'yyyy-MM-dd HH:00') as 时间段,count(*) as 报文总数,PROCERRMESG as 业务处理描述 from TABMCTL group by 时间段,业务处理描述 order by 时间段";
	String MCTLSQL2 = "按报文类型的报文总笔数##select formatdatetime(MSGDEALSTIME, 'yyyy-MM-dd HH:00') as 时间段,MSGTYPE as 报文类型,count(*) as 报文总数,PROCERRMESG as 业务处理描述 from TABMCTL group by 时间段,报文类型,业务处理描述 order by 时间段";
	String MCTLSQL3 = "单位时间内运行进程数##select formatdatetime(MSGDEALSTIME, 'yyyy-MM-dd HH:00') as 时间段,count(distinct(PID)) as 进程数 from TABMCTL group by 时间段 order by 时间段";
	String MCTLSQL4 = "单位时间内单进程处理最大报文数##select formatdatetime(MSGDEALSTIME, 'yyyy-MM-dd HH:00') as 时间段,PID as进程号,count(*) as 报文总数 from TABMCTL group by 时间段,PID order by 时间段";
	String MCTLSQL5 = "报文平均等待处理时间##select formatdatetime(MSGDEALSTIME, 'yyyy-MM-dd HH:00') as 时间段,avg(MSGWAITTIME) as 平均等待时间 from TABMCTL group by 时间段 order by 时间段";
	String MCTLSQL6 = "按报文类型统计平均等待处理时间##select formatdatetime(MSGDEALSTIME, 'yyyy-MM-dd HH:00') as 时间段,MSGTYPE as 报文类型,avg(MSGWAITTIME) as 平均等待时间 from TABMCTL group by 时间段,报文类型 order by 时间段";
	String MCTLSQL7 = "报文平均处理时间##select formatdatetime(MSGDEALSTIME, 'yyyy-MM-dd HH:00') as 时间段,avg(MSGDEALTIME) as 平均处理时间 from TABMCTL group by 时间段 order by 时间段";
	String MCTLSQL8 = "按报文类型统计报文平均处理时间##select formatdatetime(MSGDEALSTIME, 'yyyy-MM-dd HH:00') as 时间段,MSGTYPE as 报文类型,avg(MSGDEALTIME) as 平均处理时间 from TABMCTL group by 时间段,报文类型 order by 时间段";

	String PMTSNPCSQL0 = "所有记录##select * from tabpmtsnpc";
	String PMTSNPCSQL1 = "报文总数##select formatdatetime(MSGSENDTIME, 'yyyy-MM-dd HH:00') as 时间段,count(*) as 报文总数 from TABPMTSNPC group by 时间段 order by 时间段";
	String PMTSNPCSQL2 = "按报文类型的报文总笔数##select formatdatetime(MSGSENDTIME, 'yyyy-MM-dd HH:00') as 时间段,MSGTYPE as 报文类型,count(*) as 报文总数 from TABPMTSNPC group by 时间段,MSGTYPE order by 时间段";
	String PMTSNPCSQL3 = "单位时间内运行进程数##select formatdatetime(MSGSENDTIME, 'yyyy-MM-dd HH:00') as 时间段,count(distinct(PID)) as 进程数 from TABPMTSNPC group by 时间段 order by 时间段";
	String PMTSNPCSQL4 = "单位时间内单进程处理最大报文数##select formatdatetime(MSGSENDTIME, 'yyyy-MM-dd HH:00') as 时间段,PID as 进程号,count(*) as 报文总数 from TABPMTSNPC group by 时间段,进程号 order by 时间段";
	String PMTSNPCSQL5 = "报文平均处理时间##select TU.MSGTIME,TU.向上平均处理时间,TD.向下平均处理时间 from (select formatdatetime(MSGSENDTIME, 'yyyy-MM-dd HH:00') as MSGTIME,avg(MSGFWTIME) as 向上平均处理时间 from TABPMTSNPC where MSGDIRECTION = 'U' group by MSGTIME)TU,(select formatdatetime(MSGSENDTIME, 'yyyy-MM-dd HH:00') as MSGTIME,avg(MSGDEALTIME) as 向下平均处理时间 from TABPMTSNPC where MSGDIRECTION = 'D' group by MSGTIME)TD where TU.MSGTIME = TD.MSGTIME order by MSGTIME";
	String PMTSNPCSQL6 = "向上报文平均处理时间##select formatdatetime(MSGSENDTIME, 'yyyy-MM-dd HH:00') as MSGTIME,avg(MSGFWTIME) as 向上平均处理时间 from TABPMTSNPC where MSGDIRECTION = 'U' group by MSGTIME order by MSGTIME";
	String PMTSNPCSQL7 = "向下报文平均处理时间##select formatdatetime(MSGSENDTIME, 'yyyy-MM-dd HH:00') as MSGTIME,avg(MSGDEALTIME) as 向下平均处理时间 from TABPMTSNPC where MSGDIRECTION = 'D' group by MSGTIME order by MSGTIME";
	String PMTSNPCSQL8 = "按报文类型统计平均处理时间##select TU.MSGTIME,TU.MSGTYPE,TU.向上平均处理时间,TD.向下平均处理时间 from (select formatdatetime(MSGSENDTIME, 'yyyy-MM-dd HH:00') as MSGTIME,MSGTYPE,avg(MSGFWTIME) as 向上平均处理时间 from TABPMTSNPC where MSGDIRECTION = 'U' group by MSGTIME,MSGTYPE)TU,(select formatdatetime(MSGSENDTIME, 'yyyy-MM-dd HH:00') as MSGTIME,MSGTYPE,avg(MSGDEALTIME) as 向下平均处理时间 from TABPMTSNPC where MSGDIRECTION = 'D' group by MSGTIME,MSGTYPE)TD where TU.MSGTIME = TD.MSGTIME and TU.MSGTYPE = TD.MSGTYPE order by MSGTIME";
	String PMTSNPCSQL9 = "按报文类型统计向上报文平均处理时间##select formatdatetime(MSGSENDTIME, 'yyyy-MM-dd HH:00') as MSGTIME,MSGTYPE,avg(MSGFWTIME) as 向上平均处理时间 from TABPMTSNPC where MSGDIRECTION = 'U' group by MSGTIME,MSGTYPE order by MSGTIME";
	String PMTSNPCSQL10 = "按报文类型统计向下报文平均处理时间##select formatdatetime(MSGSENDTIME, 'yyyy-MM-dd HH:00') as MSGTIME,MSGTYPE,avg(MSGDEALTIME) as 向下平均处理时间 from TABPMTSNPC where MSGDIRECTION = 'D' group by MSGTIME,MSGTYPE order by MSGTIME";

	String PMTSCCPCSQL0 = "所有记录##select * from tabpmtsccpc";
	String PMTSCCPCSQL1 = "报文总数##select formatdatetime(MSGSENDTIME, 'yyyy-MM-dd HH:00') as 时间段,count(*) as 报文总数 from TABPMTSCCPC group by 时间段 order by 时间段";
	String PMTSCCPCSQL2 = "按报文类型的报文总笔数##select formatdatetime(MSGSENDTIME, 'yyyy-MM-dd HH:00') as 时间段,MSGTYPE as 报文类型,count(*) as 报文总数 from TABPMTSCCPC group by 时间段,MSGTYPE order by 时间段";
	String PMTSCCPCSQL3 = "单位时间内运行进程数##select formatdatetime(MSGSENDTIME, 'yyyy-MM-dd HH:00') as 时间段,count(distinct(PID)) as 进程数 from TABPMTSCCPC group by 时间段 order by 时间段";
	String PMTSCCPCSQL4 = "单位时间内单进程处理最大报文数##select formatdatetime(MSGSENDTIME, 'yyyy-MM-dd HH:00') as 时间段,PID as 进程号,count(*) as 报文总数 from TABPMTSCCPC group by 时间段,进程号 order by 时间段";
	String PMTSCCPCSQL5 = "报文平均处理时间##select TU.MSGTIME,TU.向上平均处理时间,TD.向下平均处理时间 from (select formatdatetime(MSGSENDTIME, 'yyyy-MM-dd HH:00') as MSGTIME,avg(MSGDEALTIME) as 向上平均处理时间 from TABPMTSCCPC where MSGDIRECTION = 'U' group by MSGTIME)TU,(select formatdatetime(MSGSENDTIME, 'yyyy-MM-dd HH:00') as MSGTIME,avg(MSGFWTIME) as 向下平均处理时间 from TABPMTSCCPC where MSGDIRECTION = 'D' group by MSGTIME)TD where TU.MSGTIME = TD.MSGTIME order by MSGTIME";
	String PMTSCCPCSQL6 = "向上报文平均处理时间##select formatdatetime(MSGSENDTIME, 'yyyy-MM-dd HH:00') as MSGTIME,avg(MSGDEALTIME) as 向上平均处理时间 from TABPMTSCCPC where MSGDIRECTION = 'U' group by MSGTIME order by MSGTIME";
	String PMTSCCPCSQL7 = "向下报文平均处理时间##select formatdatetime(MSGSENDTIME, 'yyyy-MM-dd HH:00') as MSGTIME,avg(MSGFWTIME) as 向下平均处理时间 from TABPMTSCCPC where MSGDIRECTION = 'D' group by MSGTIME order by MSGTIME";
	String PMTSCCPCSQL8 = "按报文类型统计平均处理时间##select TU.MSGTIME,TU.MSGTYPE,TU.向上平均处理时间,TD.向下平均处理时间 from (select formatdatetime(MSGSENDTIME, 'yyyy-MM-dd HH:00') as MSGTIME,MSGTYPE,avg(MSGFWTIME) as 向上平均处理时间 from TABPMTSCCPC where MSGDIRECTION = 'U' group by MSGTIME,MSGTYPE)TU,(select formatdatetime(MSGSENDTIME, 'yyyy-MM-dd HH:00') as MSGTIME,MSGTYPE,avg(MSGDEALTIME) as 向下平均处理时间 from TABPMTSCCPC where MSGDIRECTION = 'D' group by MSGTIME,MSGTYPE)TD where TU.MSGTIME = TD.MSGTIME and TU.MSGTYPE = TD.MSGTYPE order by MSGTIME";
	String PMTSCCPCSQL9 = "按报文类型统计向上报文平均处理时间##select formatdatetime(MSGSENDTIME, 'yyyy-MM-dd HH:00') as MSGTIME,MSGTYPE,avg(MSGDEALTIME) as 向上平均处理时间 from TABPMTSCCPC where MSGDIRECTION = 'U' group by MSGTIME,MSGTYPE order by MSGTIME";
	String PMTSCCPCSQL10 = "按报文类型统计向下报文平均处理时间##select formatdatetime(MSGSENDTIME, 'yyyy-MM-dd HH:00') as MSGTIME,MSGTYPE,avg(MSGFWTIME) as 向下平均处理时间 from TABPMTSCCPC where MSGDIRECTION = 'D' group by MSGTIME,MSGTYPE order by MSGTIME";

	String HELPINFO0 = "日志分析类型：支持三种日志，MCTL日志、PMTSNPC日志和PMTSCCPC日志\n\n";
	String HELPINFO1 = "数据库保存方式：MEMORY:数据库保存在内存中，DISK:数据库保存在磁盘上\n\n";
	String HELPINFO2 = "数据库名：设置数据库名，默认为test，若数据库保存方式为disk，数据库名决定本地数据库文件的名称\n\n";
	String HELPINFO3 = "本地数据库目录：设置本地数据库文件所在的目录，可用文件选择器选择本地数据库文件所在的目录，也可以直接选择本地数据库文件\n\n";
	String HELPINFO4 = "连接数据库：可以直接连接保存在的磁盘上的本地数据库，执行sql查询结果\n\n";
	String HELPINFO5 = "日志目录：选择日志所在的目录\n\n";
	String HELPINFO6 = "文件关键字：设置日志文件名的过滤机制，可以自己编辑\n\n";
	String HELPINFO7 = "启动分析：开始分析日志，写入结果到数据库中\n\n";
	String HELPINFO8 = "SQL语句：设置sql语句，可以自己编辑\n\n";
	String HELPINFO9 = "执行SQL，执行前面的sql语句\n\n";
	
	
	String[] MCTLFILTER = {  "HVPS_", "NETS_", "IBPS_", "BEPS_", "SAPS_" };
	String[] PMTSNPCFILTER = { "success." };
	String[] PMTSCCPCFILTER = { "success." };
	String[] MCTLSQL = { MCTLSQL0, MCTLSQL1, MCTLSQL2, MCTLSQL3, MCTLSQL4, MCTLSQL5, MCTLSQL6, MCTLSQL7, MCTLSQL8 };
	String[] PMTSNPCSQL = { PMTSNPCSQL0, PMTSNPCSQL1, PMTSNPCSQL2, PMTSNPCSQL3, PMTSNPCSQL4, PMTSNPCSQL5, PMTSNPCSQL6,
			PMTSNPCSQL7, PMTSNPCSQL8, PMTSNPCSQL9, PMTSNPCSQL10 };
	String[] PMTSCCPCSQL = { PMTSCCPCSQL0, PMTSCCPCSQL1, PMTSCCPCSQL2, PMTSCCPCSQL3, PMTSCCPCSQL4, PMTSCCPCSQL5, PMTSCCPCSQL6,
			PMTSCCPCSQL7, PMTSCCPCSQL8, PMTSCCPCSQL9, PMTSCCPCSQL10 };
	String HELPINFO = HELPINFO0 + HELPINFO1 + HELPINFO2 + HELPINFO3 + HELPINFO4 + HELPINFO5 + HELPINFO6
			+ HELPINFO7 + HELPINFO8 + HELPINFO9;
	

	String MCTLMSGSTART = "Level 0 : "; // 日志块开始
	String MCTLMSGEND = "] **"; // 日志块结尾
	String MCTLMQPUT = "报文放置时间";
	String MCTLMQRECV = "报文读取时间";
	String MCTLMSGDEAL = "处理报文时间";
	String MCTLPROC = "处理结果:";
	String PMTSMSGSTARTFLAG = "Level 0 PMTSMSGHDL:"; // PMTS日志开始标志
	String MSGBODYFINSHEDFLAG = "</Document>"; // 报文正文结束标志
	String PMTSPRENODETIMEFLAG = "前一节点发送时间"; // 前一节点发送时间
	String PMTSUHEADFLAG = "{U:"; // PMTS-U头标志信息
	String PMTSUENDFLAG = "}"; // PMTS-U头结尾标志信息
	String IBPSMSGHEAD = "{H:01"; // 网银报文头
	String CNAPSMSGHEAD = "{H:02"; // 二代报文头
	String CNCCMSGHEAD = "{N:01"; // CNCC报文头
}