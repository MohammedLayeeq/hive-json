import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

/**
 * This JsonInputFormat can read a multi-line JSON format. It is creating the
 * record based on balancing of curly braces - { and }. So the content between
 * first '{' to the balanced last '}' is considered as one complete record.
 * 
 * @author unayakdev
 */
public class JsonInputFormat extends TextInputFormat {

	@Override
	public RecordReader<LongWritable, Text> getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter)
			throws IOException {
		// create a new record reader.
		return new JsonRecordReader((FileSplit) inputSplit, jobConf);
	}

	public static class JsonRecordReader implements RecordReader<LongWritable, Text> {

		public static final String START_TAG_KEY = "jsoninput.start";
		public static final String END_TAG_KEY = "jsoninput.end";

		private byte[] startTag = "{".getBytes();
		private byte[] endTag = "}".getBytes();
		private long start;
		private long end;
		private FSDataInputStream fsin;
		private final DataOutputBuffer buffer = new DataOutputBuffer();

		public JsonRecordReader(FileSplit split, JobConf jobConf) throws IOException {
			// uncomment the below lines if you need to get the configuration
			// from JobConf:
			// startTag = jobConf.get(START_TAG_KEY).getBytes("utf-8");
			// endTag = jobConf.get(END_TAG_KEY).getBytes("utf-8");

			// open the file and seek to the start of the split:
			start = split.getStart();
			end = start + split.getLength();
			Path file = split.getPath();
			FileSystem fs = file.getFileSystem(jobConf);
			fsin = fs.open(split.getPath());
			fsin.seek(start);
		}

		@Override
		public boolean next(LongWritable key, Text value) throws IOException {
			if (fsin.getPos() < end) {
				AtomicInteger count = new AtomicInteger(0);
				if (readUntilMatch(false, count)) {
					try {
						buffer.write(startTag);
						if (readUntilMatch(true, count)) {
							key.set(fsin.getPos());
							// create json record from buffer:
							String jsonRecord = new String(buffer.getData(), 0, buffer.getLength());
							value.set(jsonRecord);
							return true;
						}
					} finally {
						buffer.reset();
					}
				}
			}
			return false;
		}

		@Override
		public LongWritable createKey() {
			return new LongWritable();
		}

		@Override
		public Text createValue() {
			return new Text();
		}

		@Override
		public long getPos() throws IOException {
			return fsin.getPos();
		}

		@Override
		public void close() throws IOException {
			fsin.close();
		}

		@Override
		public float getProgress() throws IOException {
			return ((fsin.getPos() - start) / (float) (end - start));
		}

		private boolean readUntilMatch(boolean withinBlock, AtomicInteger count) throws IOException {
			while (true) {
				int b = fsin.read();
				// end of file:
				if (b == -1)
					return false;

				// save to buffer:
				if (withinBlock)
					buffer.write(b);

				// check if we're matching start/end tag:
				if (b == startTag[0]) {
					count.incrementAndGet();
					if (!withinBlock) {
						return true;
					}
				} else if (b == endTag[0]) {
					count.getAndDecrement();
					if (count.get() == 0) {
						return true;
					}
				}

				// see if we've passed the stop point:
				if (!withinBlock && count.get() == 0 && fsin.getPos() >= end)
					return false;
			}
		}

	}
}
