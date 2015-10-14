import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SelectCricTeam {
	public static class GeneralBatStatsScoreMapper extends
			Mapper<Object, Text, Text, FloatWritable> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			if (line.length() > 0) {

				String[] tokens = line.split(",");

				float average = 0;
				if (!tokens[7].equals("-")) {
					average = Float.parseFloat(tokens[7]);
				}
				String hsString = tokens[6];
				float hs = 0;
				if (hsString.endsWith("*")) {
					hs = Float.parseFloat(hsString.substring(0,hsString.length() - 1));
				}
				float sr = Float.parseFloat(tokens[9]);
				int innings = Integer.parseInt(tokens[3]);
				int hundreds = Integer.parseInt(tokens[10]);
				int fifties = Integer.parseInt(tokens[11]);
				int fours = Integer.parseInt(tokens[12]);
				int sixes = Integer.parseInt(tokens[13]);

				String playerName = tokens[0];
				float weightedScore = average * 9 + hs * 3 + sr * 2 + innings
						* 3 + hundreds * 3 + fifties * 2 + sixes * 0.75f
						+ fours * 0.25f;

				System.out
						.println("GeneralBatStatsScoreMapper output vals:");
				System.out.println("BAT:" + playerName + "," + weightedScore);
				// ToRecordMap.put(playerName, new Float(weightedScore));
				context.write(new Text("BAT:" + playerName), new FloatWritable(
						weightedScore));
			}
		}
	}

	public static class GeneralBowlStatsScoreMapper extends
			Mapper<Object, Text, Text, FloatWritable> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			if (line.length() > 0) {
				
				float wickets = 0;
				String[] tokens = line.split(",");
				if (!tokens[6].equals("-")) {
					wickets = Float.parseFloat(tokens[6]);
				}
				
				float sr = 50;
				if (!tokens[11].equals("-")) {
					sr = Float.parseFloat(tokens[11]);
				}
				
				float avg=50;
				if (!tokens[9].equals("-")) {
					avg = Float.parseFloat(tokens[9]);
				}
				
				float econ=6;
				if (!tokens[10].equals("-")) {
					econ = Float.parseFloat(tokens[10]);
				}
				
				int fourWkts=0;
				if (!tokens[12].equals("-")) {
					fourWkts = Integer.parseInt(tokens[12]);
				}
				
				int fiveWkts=0;
				if (!tokens[13].equals("-")) {
					fiveWkts = Integer.parseInt(tokens[13]);
				}
				
				int innings=0;
				if (!tokens[3].equals("-")) {
					innings = Integer.parseInt(tokens[3]);
				}
				
				String playerName = tokens[0];
				float weightedScore = wickets * 5 - econ*20 -avg*3- sr*3+fiveWkts*10+fourWkts ; //not finished writing yet
				System.out.println("GeneralBowlStatsScoreMapper output vals:");
				System.out.println("BOWL:" + playerName + "," + weightedScore);
				// ToRecordMap.put(playerName, new Float(weightedScore));
				context.write(new Text("BOWL:" + playerName),
						new FloatWritable(weightedScore));
			}
		}
	}

	public static class VsOppositionBatStatsScoreMapper extends
			Mapper<Object, Text, Text, FloatWritable> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			if (line.length() > 0) {

				String[] tokens = line.split(",");
				if (tokens.length > 1) {
					
					float average = 0;
					if (!tokens[6].equals("-")) {
						average = Float.parseFloat(tokens[6]);
					}
					String hsString = tokens[5];
					float hs = 0;
					if (hsString.endsWith("*")) {
						hs = Float.parseFloat(hsString.substring(0,hsString.length() - 1));
						System.out.println("highest score taken:"+hs+"\n");
					}
					
					float sr =0;
					if (!tokens[8].equals("-")) {
						sr= Float.parseFloat(tokens[8]);
					}
					
					int innings =0;
					if (!tokens[2].equals("-")) {
						innings= Integer.parseInt(tokens[2]);
					}
					
					int hundreds=0;
					if (!tokens[9].equals("-")) {
						hundreds= Integer.parseInt(tokens[9]);
					}
					
					int fifties =0;
					if (!tokens[10].equals("-")) {
						fifties= Integer.parseInt(tokens[10]);
					}
					
					int fours=0;
					if (!tokens[12].equals("-")) {
						fours= Integer.parseInt(tokens[12]);
					}
					
					int sixes =0;
					if (!tokens[13].equals("-")) {
						sixes= Integer.parseInt(tokens[13]);
					}
					
					String playerName = tokens[0];
					float weightedScore = average * 11 + hs *4  + sr * 5 + innings
							* 3 + hundreds * 4 + fifties * 2 + sixes * 0.75f
							+ fours * 0.25f;
					
					System.out.println("VsOppositionBatStatsScoreMapper output vals:");
					System.out.println("BAT:" + playerName + ","
							+ weightedScore);
					// ToRecordMap.put(playerName, new Float(weightedScore));
					context.write(new Text("BAT:" + playerName),
							new FloatWritable(weightedScore));
				}
			}
		}
	}

	// common reducer for all the mapper types
	public static class CommmonReducer extends
			Reducer<Text, FloatWritable, NullWritable, Text> {
		private FloatWritable result = new FloatWritable();
		private TreeMap<Float, Text> topBatsmenMap = new TreeMap<Float, Text>();
		private TreeMap<Float, Text> topBowlersMap = new TreeMap<Float, Text>();

		// private LinkedHashMap<Float, Text> topScoresLHMap=new
		// LinkedHashMap<Float, Text>()
		public void reduce(Text key, Iterable<FloatWritable> values,// key=player
																	// name
				Context context) throws IOException, InterruptedException {

			Text newKey = new Text(key);
			float scoreSum = 0;
			for (FloatWritable value : values) { // for each line
				float val = value.get();
				scoreSum += val;
			}
			// To a is a batsman record
			if (key.toString().startsWith("BAT:")) {
				System.out
						.println("Putting Values in Reducer into topBatsmenMap:"
								+ "map key=" + scoreSum + ",Map Val=" + key);
				topBatsmenMap.put(scoreSum, newKey);// this toScoreMap's key is
													// the
				// scoresum and value is
				// player's name

				if (topBatsmenMap.size() > 6) {
					System.out.println("removed entry.");
					topBatsmenMap.remove(topBatsmenMap.firstKey());
				}
				// To a bowler record
			} else if (key.toString().startsWith("BOWL:")) {
				System.out
						.println("Putting Values in Reducer into topBowlersMap:"
								+ "map key=" + scoreSum + ",Map Val=" + key);
				topBowlersMap.put(scoreSum, newKey);// this toScoreMap's key is
													// the
				// scoresum and value is
				// player's name

				if (topBowlersMap.size() > 5) {
					System.out.println("removed entry.");
					topBowlersMap.remove(topBowlersMap.firstKey());
				}
			}

		}

		// org.apache.hadoop.mapreduce.Reducer<Text,FloatWritable,NullWritable,Text>.
		protected void cleanup(Context context) throws IOException, // cleanup
																	// runs only
																	// once
																	// after all
																	// the map
																	// functions
																	// for all
																	// keys were
																	// executed
				InterruptedException {

			System.out.println("\n\nCleanup running once.");
			context.write(NullWritable.get(), new Text(
					"Selected Batsmen in the order of best performence\n=============================================\n\n"));
			// for batting
			for (Entry<Float, Text> entry : topBatsmenMap.descendingMap()
					.entrySet()) {
				// Output our ten records to the file system with a null key
				Text newText = new Text(entry.getKey() + "," + entry.getValue());
				System.out.println(newText);
				context.write(NullWritable.get(), newText);
			}

			System.out.println("\n");
			context.write(NullWritable.get(), new Text(
					"\n\nSelected Bowlers in the order of best performance\n=============================================\n\n"));
			// for bowling
			for (Entry<Float, Text> entry : topBowlersMap.descendingMap()
					.entrySet()) {
				// Output our ten records to the file system with a null key
				Text newText = new Text(entry.getKey() + "," + entry.getValue());
				System.out.println(newText);
				context.write(NullWritable.get(), newText);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "number sum");
		job.setJarByClass(SelectCricTeam.class);
		job.setReducerClass(CommmonReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);

		MultipleInputs.addInputPath(job, new Path(args[1]),
				TextInputFormat.class, GeneralBatStatsScoreMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[2]),
				TextInputFormat.class, VsOppositionBatStatsScoreMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[3]),
				TextInputFormat.class, GeneralBowlStatsScoreMapper.class);

		FileOutputFormat.setOutputPath(job, new Path(args[0]));
		System.exit(job.waitForCompletion(false) ? 0 : 1);

	}
}