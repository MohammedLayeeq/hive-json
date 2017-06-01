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

import com.google.gson.Gson;

/**
 * Reads JSON records that are balanced between start and end braces.
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
		public static final String RECORD_DELIMITER = "jsoninput.recordelim";
		public static final String COLLECTION_DELIMITER = "jsoninput.collectiondelim";

		private byte[] startTag = "{".getBytes();
		private byte[] endTag = "}".getBytes();
		private String recordDelimiter = ",";
		private String collectionDelimiter = "|";
		private long start;
		private long end;
		private FSDataInputStream fsin;
		private final DataOutputBuffer buffer = new DataOutputBuffer();

		public JsonRecordReader(FileSplit split, JobConf jobConf) throws IOException {
			// uncomment the below lines if you need to get the configuration from jobConf:
			// startTag = jobConf.get(START_TAG_KEY).getBytes("utf-8");
			// endTag = jobConf.get(END_TAG_KEY).getBytes("utf-8");
			// recordDelimiter = jobConf.get(RECORD_DELIMITER);
			// collectionDelimiter = jobConf.get(COLLECTION_DELIMITER);
			
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
							// parse the json to text:
							String txt = parseToString(jsonRecord);
							value.set(txt);
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

				// check if we're matching:
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

		private String parseToString(String json) {
			Gson gson = new Gson();
			// create POJO based object from the json record:
			Book book = gson.fromJson(json, Book.class);

			// create a simple text based structure from json using configured
			// delimiters:
			StringBuilder result = new StringBuilder();
			result.append(book.getId());
			result.append(recordDelimiter);
			result.append(book.getBookname());
			result.append(recordDelimiter);
			result.append(book.getProperties().getSubscription());
			result.append(collectionDelimiter);
			result.append(book.getProperties().getUnit());
			return result.toString();
		}

	}
}
