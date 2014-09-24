import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SnappyCodec;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;
import java.util.ArrayList;
import java.util.List;

public class SequenceFileBuilder {
    private SequenceFile.Writer output;

    public SequenceFileBuilder(File outputFile, CompressionCodec codec) throws Exception {
        Configuration conf = new Configuration();

        conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");

        output = SequenceFile.createWriter(FileSystem.get(conf), conf,
                                           new Path(outputFile.getAbsolutePath()),
                                           Text.class, Text.class,
                                           SequenceFile.CompressionType.BLOCK,
                                           codec);
    }

    public void append(File f) throws IOException {
        if (f.isDirectory()) {
            append(Arrays.asList(f.listFiles()));
            return;
        }

        BufferedReader br = new BufferedReader(getReader(f));

        Text key = new Text(f.getAbsolutePath());

        String line;
        while ((line = br.readLine()) != null) {
            output.append(key, new Text(line));
        }

        br.close();
    }

    public void append(Iterable<File> files) throws IOException {
        for (File f : files) {
            append(f);
        }
    }

    private InputStreamReader getReader(File f) throws FileNotFoundException, IOException {
        if (f.getAbsolutePath().endsWith(".gz")) {
            return new InputStreamReader(new GZIPInputStream(new FileInputStream(f)));
        } else {
            System.err.println("Can't handle input file: " + f.getAbsolutePath());
            System.exit(1);
        }
        return null;
    }

    public void close() throws IOException {
        output.close();
    }

    public static void main(String[] args) throws Exception  {
        ArrayList<File> inputFiles = new ArrayList<File>();
        File outputFile = null;

        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-o")) {
                outputFile = new File(args[i + 1]);
                i++;
            } else {
                inputFiles.add(new File(args[i]));
            }
        }

        if (outputFile == null) {
            System.err.println("Must specify an output file");
            System.exit(1);
        }

        SequenceFileBuilder b = new SequenceFileBuilder(outputFile, new SnappyCodec());

        b.append(inputFiles);

        b.close();
    }
}
