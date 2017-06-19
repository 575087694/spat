package spat;

import java.awt.Dimension;
import java.awt.Font;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.swing.BorderFactory;
import javax.swing.ButtonGroup;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.UIManager;
import javax.swing.plaf.metal.MetalLookAndFeel;
import javax.swing.plaf.metal.OceanTheme;

import tools.Data;
import tools.MyChart;
import tools.UtilTools;

public class RunUI implements Data {
	private final int winWidth = 900;
	private final int winHeight = 720;
	final JTextArea textArea = new JTextArea();
	Connection dbconn = null;

	List<Date> lastDate = new ArrayList<Date>();
	List<Date> nowDate = new ArrayList<Date>();
	List<Double> lastData = new ArrayList<Double>();
	List<Double> nowData = new ArrayList<Double>();
	
	public RunUI() {
		JFrame frame = new JFrame("交易系统日志分析工具");
		JPanel panelContainer = new JPanel();
		panelContainer.setLayout(new GridLayout(0, 1));

		// 菜单栏
		JMenuBar menuBar = new JMenuBar();
		menuBar.setOpaque(true);
		JMenu hitsMenu = new JMenu("帮助");
		JMenuItem item = new JMenuItem("使用说明");
		hitsMenu.add(item);
		menuBar.add(hitsMenu);
		hitsMenu.setToolTipText("查看使用说明");
		hitsMenu.setMnemonic(KeyEvent.VK_H);
		frame.setJMenuBar(menuBar);

		JPanel panelTop = new JPanel();
		panelTop.setLayout(new GridLayout(0, 1));
		JPanel panelBottom = new JPanel();
		panelBottom.setLayout(new GridLayout(0, 1));
		panelContainer.add(panelTop);
		panelContainer.add(panelBottom);

		// 分析类别栏
		JPanel typePanel = new JPanel();
		typePanel.setLayout(null);
		typePanel.setBorder(BorderFactory.createTitledBorder("日志类型设置"));
		typePanel.setSize(winWidth, winHeight / 6);
		typePanel.setBounds(0, 0, winWidth - 20, winHeight / 7);
		final JRadioButton[] typeButton = new JRadioButton[3];
		typeButton[0] = new JRadioButton("MCTL日志", true);
		typeButton[1] = new JRadioButton("PMTSNPC日志");
		typeButton[2] = new JRadioButton("PMTSCCPC日志");
		ButtonGroup buttonGroup = new ButtonGroup();
		buttonGroup.add(typeButton[0]);
		buttonGroup.add(typeButton[1]);
		buttonGroup.add(typeButton[2]);
		buttonGroup.setSelected(null, true);
		typeButton[0].setFont(new Font(null, Font.ITALIC, 12));
		typeButton[1].setFont(new Font(null, Font.ITALIC, 12));
		typeButton[2].setFont(new Font(null, Font.ITALIC, 12));
		typeButton[0].setBounds(10, 25, 120, winHeight / 24);
		typeButton[1].setBounds(140, 25, 120, winHeight / 24);
		typeButton[2].setBounds(270, 25, 120, winHeight / 24);
		typePanel.add(typeButton[0]);
		typePanel.add(typeButton[1]);
		typePanel.add(typeButton[2]);
		JButton chartButton = new JButton("画图");
		chartButton.setBounds(725, 25, 100, winHeight / 20);
		typePanel.add(chartButton);
		panelTop.add(typePanel);

		// 数据库配置栏
		JPanel databasePanel = new JPanel();
		databasePanel.setLayout(null);
		databasePanel.setBorder(BorderFactory.createTitledBorder("数据库保存方式"));
		databasePanel.setSize(winWidth, winHeight / 6);
		databasePanel.setBounds(0, 0, winWidth - 20, winHeight / 7);
		final JRadioButton[] dataButton = new JRadioButton[3];
		dataButton[0] = new JRadioButton("MEMORY", true);
		dataButton[1] = new JRadioButton("DISK");
		ButtonGroup buttonGroup2 = new ButtonGroup();
		buttonGroup2.add(dataButton[0]);
		buttonGroup2.add(dataButton[1]);
		buttonGroup2.setSelected(null, true);
		dataButton[0].setFont(new Font(null, Font.ITALIC, 12));
		dataButton[1].setFont(new Font(null, Font.ITALIC, 12));
		dataButton[0].setBounds(10, 30, 90, winHeight / 24);
		dataButton[1].setBounds(140, 30, 90, winHeight / 24);
		JPanel publicPanel = new JPanel();
		publicPanel.setLayout(null);
		publicPanel.setSize(winWidth - 240, winHeight / 12);
		publicPanel.setBounds(200, 15, winWidth - 240, winHeight / 12);
		JLabel dbLabel0 = new JLabel("数据库名:");
		dbLabel0.setBounds(35, 0, 60, winHeight / 12);
		JTextField dbText0 = new JTextField("");
		dbText0.setBounds(95, 10, 60, winHeight / 20);
		JLabel dbLabel = new JLabel("本地数据库目录:");
		dbLabel.setBounds(200, 0, 90, winHeight / 12);
		JTextField dbText1 = new JTextField("");
		dbText1.setBounds(300, 10, 115, winHeight / 20);
		JButton dbButton = new JButton("...");
		dbButton.setBounds(425, 10, 50, winHeight / 20);
		JButton confButton = new JButton("连接数据库");
		confButton.setBounds(525, 10, 100, winHeight / 20);
		publicPanel.add(dbLabel0);
		publicPanel.add(dbText0);
		publicPanel.add(dbLabel);
		publicPanel.add(dbText1);
		publicPanel.add(dbButton);
		publicPanel.add(confButton);
		databasePanel.add(dataButton[0]);
		databasePanel.add(dataButton[1]);
		databasePanel.add(publicPanel);
		publicPanel.setVisible(false);
		panelTop.add(databasePanel);
		panelTop.add(databasePanel);

		// 选择目录栏
		JPanel timePanel = new JPanel();
		timePanel.setBorder(javax.swing.BorderFactory.createTitledBorder("日志配置"));
		timePanel.setBounds(0, winHeight / 6 + 5, winWidth - 20, winHeight / 6);//
		timePanel.setLayout(null);
		JLabel dirLabel = new JLabel("日志目录:");
		dirLabel.setBounds(15, 15, winWidth / 3 + 40, winHeight / 12);
		JTextField dirText = new JTextField("");
		dirText.setBounds(75, 25, winWidth / 3, winHeight / 20);
		JButton dirButton = new JButton("...");
		dirButton.setBounds(385, 25, 50, winHeight / 20);
		JLabel filekeyLabel = new JLabel("文件关键字:");
		filekeyLabel.setBounds(465, 15, winWidth / 3 + 40, winHeight / 12);
		JComboBox<String> filekeyBox = new JComboBox<String>();
		filekeyBox.setEditable(true);
		filekeyBox.setBounds(535, 25, 150, winHeight / 20);
		changeView(filekeyBox, MCTLFILTER);
		JButton startButton = new JButton("启动分析");
		startButton.setBounds(725, 25, 100, winHeight / 20);
		timePanel.add(dirLabel);
		timePanel.add(dirText);
		timePanel.add(dirButton);
		timePanel.add(startButton);
		timePanel.add(filekeyBox);
		timePanel.add(filekeyLabel);
		panelTop.add(timePanel);

		// SQL设置栏
		JPanel sqlPanel = new JPanel();
		sqlPanel.setBorder(javax.swing.BorderFactory.createTitledBorder("SQL设置"));
		sqlPanel.setLayout(null);
		sqlPanel.setBounds(0, winHeight / 6 + 5, winWidth - 20, winHeight / 6);
		JLabel sqlLabel = new JLabel("SQL语句:");
		sqlLabel.setBounds(15, 25, 100, winHeight / 20);
		JComboBox<String> sqlText = new JComboBox<String>();
		sqlText.setEditable(true);
		changeView(sqlText, MCTLSQL);
		sqlText.setBounds(70, 25, 615, winHeight / 20);
		JButton sqlButton = new JButton("执行SQL");
		sqlButton.setBounds(725, 25, 100, winHeight / 20);
		timePanel.add(sqlButton);
		sqlPanel.add(sqlLabel);
		sqlPanel.add(sqlText);
		sqlPanel.add(sqlButton);
		panelTop.add(sqlPanel);

		// 输出窗口栏
		JPanel ioPanel = new JPanel();
		ioPanel.setBorder(javax.swing.BorderFactory.createTitledBorder("输出窗口"));
		ioPanel.setBounds(0, 2 * (winHeight / 6) + 85, winWidth - 20, winHeight / 3 + 10);//
		ioPanel.setLayout(new GridLayout(1, 1));
		textArea.setBounds(0, 5, winWidth - 20, winHeight / 3 - 20);
		ioPanel.add(textArea);
		JScrollPane scroll = new JScrollPane(textArea);
		// 分别设置水平和垂直滚动条总是出现
		scroll.setHorizontalScrollBarPolicy(JScrollPane.HORIZONTAL_SCROLLBAR_ALWAYS);
		scroll.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
		ioPanel.add(scroll);
		panelBottom.add(ioPanel);

		// 选择本地数据库文件按钮
		dbButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent event) {
				JFileChooser fc = new JFileChooser(new File("./"));// 设置默认打开当前路径
				fc.setFileSelectionMode(JFileChooser.FILES_AND_DIRECTORIES);// 可以选择目录和文件
				int flag = fc.showOpenDialog(null);
				if (flag == JFileChooser.APPROVE_OPTION) {
					File f = fc.getSelectedFile();
					String filepath = f.getPath();
					if (f.isDirectory()) {
						dbText1.setText(filepath);
					} else if (f.isFile()) {
						int pos = filepath.lastIndexOf("\\");
						if (-1 != pos) {
							String path = filepath.substring(0, pos);
							String filename = filepath.substring(pos + 1);
							int pos2 = filename.indexOf(".mv.db");
							if (-1 != pos2) {
								String dbname = filename.substring(0, pos2);
								dbText0.setText(dbname);
								dbText1.setText(path);
							}
						}
					} else {
						textArea.append("未知错误！\n");
					}
				}
			}
		});

		// 日志类型选择按钮
		typeButton[0].addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent event) {
				changeView(filekeyBox, MCTLFILTER);
				changeView(sqlText, MCTLSQL);
			}
		});
		typeButton[1].addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent event) {
				changeView(filekeyBox, PMTSNPCFILTER);
				changeView(sqlText, PMTSNPCSQL);
			}
		});
		typeButton[2].addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent event) {
				changeView(filekeyBox, PMTSCCPCFILTER);
				changeView(sqlText, PMTSCCPCSQL);
			}
		});

		// 数据库保存方式按钮(点击显示隐藏的选项)
		dataButton[0].addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				publicPanel.setVisible(false);
			}
		});
		dataButton[1].addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				publicPanel.setVisible(true);
			}
		});

		// 选择日志目录按钮
		dirButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent event) {
				JFileChooser fc = new JFileChooser(new File("./"));// 设置默认打开当前路径
				fc.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);// 只能选择目录
				int flag = fc.showOpenDialog(null);
				if (flag == JFileChooser.APPROVE_OPTION) {
					File f = fc.getSelectedFile();
					String path = f.getPath();
					dirText.setText(path);
				}
			}
		});

		// 确定按钮
		confButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent event) {
				dbconn = UtilTools.createConnection(dbText0.getText(), dataButton[0].isSelected(), dbText1.getText());
			}
		});

		// 分析日志按钮
		startButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent event) {
				long aa = System.currentTimeMillis();
				dbconn = UtilTools.createConnection(dbText0.getText(), dataButton[0].isSelected(), dbText1.getText());
				String filepath = dirText.getText();
				String filter = filekeyBox.getSelectedItem().toString();
				if (typeButton[0].isSelected()) {
					MCTL mctl = new MCTL(dbconn, filepath, filter, textArea);
					mctl.runMainMethod();
				} else if (typeButton[1].isSelected()) {
					PMTSNPC pmtsnpc = new PMTSNPC(dbconn, filepath, filter, textArea);
					pmtsnpc.runMainMethod();
				} else if ((typeButton[2].isSelected())) {
					PMTSCCPC pmtsccpc = new PMTSCCPC(dbconn, filepath, filter, textArea);
					pmtsccpc.runMainMethod();
				} else {

				}
				long bb = System.currentTimeMillis();
				System.out.println(bb - aa);
				textArea.append("日志分析完毕\n");
			}
		});

		// 执行sql语句按钮
		sqlButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent event) {
				try {
					String sql = sqlText.getSelectedItem().toString();
					textArea.append(sql + "\n");
					if (sql.indexOf("##") != -1) {
						sql = sql.substring(sql.indexOf("##") + 2, sql.length());
					}
					Statement stmt = dbconn.createStatement();
					ResultSet rs = stmt.executeQuery(sql);
					ResultSetMetaData rsmd = rs.getMetaData();
					
					boolean flag = false;
					lastDate.clear();
					lastData.clear();
					lastDate.addAll(nowDate);
					lastData.addAll(nowData);
					nowDate.clear();
					nowData.clear();
					DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:00");
					if (rsmd.getColumnTypeName(2).indexOf("INT") != -1) {
						flag = true;
					}
					
					while (rs.next()) {
						for (int i = 1; i <= rsmd.getColumnCount(); i++) {
							textArea.append(rsmd.getColumnName(i) + ":" + rs.getString(rsmd.getColumnName(i)) + "    ");
						}
						textArea.append("\n");
						
						if (flag) {
							nowDate.add(sdf.parse(rs.getString(rsmd.getColumnName(1))));
							nowData.add(Double.parseDouble(rs.getString(rsmd.getColumnName(2))));
						}
					}
					textArea.append("\n");
					rs.close();
					stmt.close();
				} catch (SQLException e) {
					textArea.append("查询语句错误!\n\n");
					e.printStackTrace();
				} catch (ParseException e){
					e.printStackTrace();
				}
			}
		});

		// 帮助信息
		item.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent event) {
				textArea.setText(HELPINFO);
			}
		});
		
		//制图
		chartButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				MyChart chart = new MyChart();
				if(!lastDate.isEmpty()&& !lastData.isEmpty()){
					chart.add(lastDate, lastData);	
				}
				if(!nowDate.isEmpty()&& !nowData.isEmpty()){
					chart.add(nowDate, nowData);	
				}
				chart.DisplayChart();
			}
		});

		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frame.setSize(new Dimension(winWidth, winHeight));
		frame.setContentPane(panelContainer);
		frame.setVisible(true);
		frame.setLocationRelativeTo(null);// 设置窗口在桌面上居中
	}

	// 显示sql语句
	public static void changeView(JComboBox<String> combobox, String[] list) {
		combobox.removeAllItems();
		for (int i = 0; i < list.length; i++) {
			combobox.addItem(list[i]);
		}
	}

	// 程序入口
	public static void main(String[] agrs) {
		try {
			MetalLookAndFeel.setCurrentTheme(new OceanTheme());
			UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
		} catch (Exception e) {
			e.printStackTrace();
		}
		new RunUI();
	}
}