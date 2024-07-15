package gr.aueb.delorean.chimp.benchmarks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

import gr.aueb.delorean.chimp.Chimp32;
import gr.aueb.delorean.chimp.ChimpDecompressor32;
import gr.aueb.delorean.chimp.ChimpN32;
import gr.aueb.delorean.chimp.ChimpNDecompressor32;
import gr.aueb.delorean.chimp.Value;



/**
 * These are generic tests to test that input matches the output after compression + decompression cycle, using
 * the value compression.
 *
 */
public class TestSinglePrecision {

	private static final int MINIMUM_TOTAL_BLOCKS = 50_000;
	private static String[] FILENAMES = {
	        "/city_temperature.csv.gz",
	        "/Stocks-Germany-sample.txt.gz",
	        "/SSD_HDD_benchmarks.csv.gz"
			};



	@Test
    public void testChimpN32() throws IOException {
        int previousValues = 64;

        for (String filename : FILENAMES) {
            TimeseriesFileReader timeseriesFileReader = new TimeseriesFileReader(getClass().getResourceAsStream(filename));
            long totalSize = 0;
            float totalBlocks = 0;
            double[] values;
            long encodingDuration = 0;
            long decodingDuration = 0;
            while ((values = timeseriesFileReader.nextBlock()) != null || totalBlocks < MINIMUM_TOTAL_BLOCKS) {
                if (values == null) {
                    timeseriesFileReader = new TimeseriesFileReader(getClass().getResourceAsStream(filename));
                    values = timeseriesFileReader.nextBlock();
                }
                ChimpN32 compressor = new ChimpN32(previousValues);
                long start = System.nanoTime();
                for (double value : values) {
                    compressor.addValue((float) value);
                }
                compressor.close();
                encodingDuration += System.nanoTime() - start;
                totalSize += compressor.getSize();
                totalBlocks += 1;

                ChimpNDecompressor32 d = new ChimpNDecompressor32(compressor.getOut(), previousValues);
                for(Double value : values) {
                    start = System.nanoTime();
                    Float pair = d.readValue();
                    decodingDuration += System.nanoTime() - start;
                  assertEquals(value.floatValue(), pair.floatValue(), "Value did not match");
                }
              assertNull(d.readValue());

            }
            System.out.println(String.format("Chimp32-64: %s - Bits/value: %.2f, Compression time per block: %.2f, Decompression time per block: %.2f", filename, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE), encodingDuration / totalBlocks, decodingDuration / totalBlocks));
        }
    }


	@Test
    public void testChimp32() throws IOException {
        for (String filename : FILENAMES) {
            TimeseriesFileReader timeseriesFileReader = new TimeseriesFileReader(getClass().getResourceAsStream(filename));
            long totalSize = 0;
            float totalBlocks = 0;
            double[] values;
            long encodingDuration = 0;
            long decodingDuration = 0;
            while ((values = timeseriesFileReader.nextBlock()) != null || totalBlocks < MINIMUM_TOTAL_BLOCKS) {
                if (values == null) {
                    timeseriesFileReader = new TimeseriesFileReader(getClass().getResourceAsStream(filename));
                    values = timeseriesFileReader.nextBlock();
                }
                Chimp32 compressor = new Chimp32();
                long start = System.nanoTime();
                for (double value : values) {
                    compressor.addValue((float) value);
                }
                compressor.close();
                encodingDuration += System.nanoTime() - start;
                totalSize += compressor.getSize();
                totalBlocks += 1;

                ChimpDecompressor32 d = new ChimpDecompressor32(compressor.getOut());
                for(Double value : values) {
                    start = System.nanoTime();
                    Value pair = d.readPair();
                    decodingDuration += System.nanoTime() - start;
                    assertEquals(value.floatValue(), pair.getFloatValue(), "Value did not match");
                }
                assertNull(d.readPair());

            }
            System.out.println(String.format("Chimp32: %s - Bits/value: %.2f, Compression time per block: %.2f, Decompression time per block: %.2f", filename, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE), encodingDuration / totalBlocks, decodingDuration / totalBlocks));
        }
    }

    public static float[] toFloatArray(byte[] byteArray){
        int times = Float.SIZE / Byte.SIZE;
        float[] floats = new float[byteArray.length / times];
        for(int i=0;i<floats.length;i++){
            floats[i] = ByteBuffer.wrap(byteArray, i*times, times).getFloat();
        }
        return floats;
    }


}
