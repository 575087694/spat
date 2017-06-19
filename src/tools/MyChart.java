package tools;

import java.util.Date;
import java.util.List;

import javax.swing.JFrame;
import javax.swing.JPanel;

import org.knowm.xchart.XChartPanel;
import org.knowm.xchart.XYChart;
import org.knowm.xchart.XYChartBuilder;
import org.knowm.xchart.style.Styler.LegendPosition;
import org.knowm.xchart.style.markers.SeriesMarkers;

public class MyChart{

	XYChart chart = null;
	int i = 0;
	
	public MyChart(){
		chart = new XYChartBuilder().width(800).height(600).xAxisTitle("X").yAxisTitle("Y").title("曲线图")
				.build();
		chart.getStyler().setLegendVisible(true);
		chart.getStyler().setLegendPosition(LegendPosition.InsideNW);
	}

	public void add(List<Date> date, List<Double> Data){
		i++;
		chart.addSeries("图"+i, date, Data).setMarker(SeriesMarkers.NONE);
	}
	
	public void DisplayChart(){
		final JFrame frame = new JFrame("XYchart");
		javax.swing.SwingUtilities.invokeLater(new Runnable() {
			@Override
			public void run() {
				frame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
				JPanel chartPanel = new XChartPanel<XYChart>(chart);
				frame.add(chartPanel);
				frame.pack();
				frame.setVisible(true);
			}
		});
	}
}
