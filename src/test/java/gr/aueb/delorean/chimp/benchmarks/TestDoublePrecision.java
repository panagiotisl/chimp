package gr.aueb.delorean.chimp.benchmarks;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.junit.jupiter.api.Test;

import gr.aueb.delorean.chimp.Chimp;
import gr.aueb.delorean.chimp.ChimpDecompressor;
import gr.aueb.delorean.chimp.ChimpN;
import gr.aueb.delorean.chimp.ChimpNDecompressor;
import gr.aueb.delorean.chimp.ChimpNNoIndex;

/**
 * These are generic tests to test that input matches the output after compression + decompression cycle, using
 * the value compression.
 *
 */
public class TestDoublePrecision {

	private static final int MINIMUM_TOTAL_BLOCKS = 50_000;
	private static String[] FILENAMES = {
	        "/city_temperature.csv.gz",
	        "/Stocks-Germany-sample.txt.gz",
	        "/SSD_HDD_benchmarks.csv.gz"
			};

	@Test
	public void testChimp128() throws IOException {
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
				ChimpN compressor = new ChimpN(128);
				long start = System.nanoTime();
				for (double value : values) {
					compressor.addValue(value);
				}
		        compressor.close();
		        encodingDuration += System.nanoTime() - start;
		        totalSize += compressor.getSize();
		        totalBlocks += 1;

				ChimpNDecompressor d = new ChimpNDecompressor(compressor.getOut(), 128);
				start = System.nanoTime();
				List<Double> uncompressedValues = d.getValues();
				decodingDuration += System.nanoTime() - start;
				for(int i=0; i<values.length; i++) {
		            assertEquals(values[i], uncompressedValues.get(i).doubleValue(), "Value did not match");
		        }


			}
			System.out.println(String.format("Chimp128: %s - Bits/value: %.2f, Compression time per block: %.2f, Decompression time per block: %.2f", filename, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE), encodingDuration / totalBlocks, decodingDuration / totalBlocks));
		}
	}

	@Test
    public void testChimp() throws IOException {
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
                Chimp compressor = new Chimp();
                long start = System.nanoTime();
                for (double value : values) {
                    compressor.addValue(value);
                }
                compressor.close();
                encodingDuration += System.nanoTime() - start;
                totalSize += compressor.getSize();
                totalBlocks += 1;

                ChimpDecompressor d = new ChimpDecompressor(compressor.getOut());
                start = System.nanoTime();
                List<Double> uncompressedValues = d.getValues();
                decodingDuration += System.nanoTime() - start;
                for(int i=0; i<values.length; i++) {
                    assertEquals(values[i], uncompressedValues.get(i).doubleValue(), "Value did not match");
                }

            }
            System.out.println(String.format("Chimp: %s - Bits/value: %.2f, Compression time per block: %.2f, Decompression time per block: %.2f", filename, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE), encodingDuration / totalBlocks, decodingDuration / totalBlocks));
        }
    }

	   @Test
	    public void testChimp128NoIndex() throws IOException {
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
	                ChimpNNoIndex compressor = new ChimpNNoIndex(128);
	                long start = System.nanoTime();
	                for (double value : values) {
	                    compressor.addValue(value);
	                }
	                compressor.close();
	                encodingDuration += System.nanoTime() - start;
	                totalSize += compressor.getSize();
	                totalBlocks += 1;

	                ChimpNDecompressor d = new ChimpNDecompressor(compressor.getOut(), 128);
	                start = System.nanoTime();
	                List<Double> uncompressedValues = d.getValues();
	                decodingDuration += System.nanoTime() - start;
	                for(int i=0; i<values.length; i++) {
	                    assertEquals(values[i], uncompressedValues.get(i).doubleValue(), "Value did not match");
	                }


	            }
	            System.out.println(String.format("Chimp128-no-index: %s - Bits/value: %.2f, Compression time per block: %.2f, Decompression time per block: %.2f", filename, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE), encodingDuration / totalBlocks, decodingDuration / totalBlocks));
	        }
	    }

    public static double[] toDoubleArray(byte[] byteArray){
        int times = Double.SIZE / Byte.SIZE;
        double[] doubles = new double[byteArray.length / times];
        for(int i=0;i<doubles.length;i++){
            doubles[i] = ByteBuffer.wrap(byteArray, i*times, times).getDouble();
        }
        return doubles;
    }

}
