package fileLoader;

import java.awt.image.BufferedImage;
import java.awt.image.DataBuffer;
import java.awt.image.DataBufferByte;
import java.awt.image.WritableRaster;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;


public class LoadMultiStack
{
    public static void saveImages(byte[][][] matrix, String fileName) {

		try {
			int zDim = matrix.length;
			for (int iz = 0; iz < zDim; iz++) {
				byte[][] stack = matrix[iz];
				int xDim = stack.length;
				int yDim = stack[0].length;
				byte[] array = new byte[xDim * yDim];
				BufferedImage b = new BufferedImage(xDim, yDim, BufferedImage.TYPE_BYTE_GRAY);
				WritableRaster r = b.getRaster();
				for (int y = 0; y < yDim; y++) {
					for (int x = 0; x < xDim; x++) {
						array[y * xDim + x] = stack[x][y];
					}
				}			
				r.setDataElements(0, 0, xDim, yDim, array);
				String filename = fileName + "_layer" + iz + ".tiff";
				File file = new File(filename);
				file.getParentFile().mkdirs();
				ImageIO.write(b, "TIFF", new File(filename));
				System.out.println(filename + "saved");
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

    public static byte[][] loadImage(String fileName, int xDim, int yDim, int zDim) {

		try {
			final File imageFile = checkFile(fileName);		
			final ImageReader reader = buildReader(imageFile, zDim);
	        final byte[][] imageBytes = new byte[zDim][xDim * yDim];

			for (int iz = 0; iz < zDim; iz++) {
				final BufferedImage image = reader.read(iz);
		        final DataBuffer dataBuffer = image.getRaster().getDataBuffer();
		        final byte layerBytes[] = ((DataBufferByte)dataBuffer).getData();
				System.arraycopy(layerBytes, 0, imageBytes[iz], 0, xDim * yDim);
			}
	        
	        return imageBytes;

		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
    
    public static byte[] loadDist(String fileName, int xDim, int yDim, int zDim) {

		try {
			final File imageFile = checkFile(fileName);		
			final ImageReader reader = buildReader(imageFile, zDim);
	        final byte imageBytes[] = new byte[xDim * yDim * zDim];

			for (int ix = 0; ix < zDim; ix++) {
				final BufferedImage image = reader.read(ix);
		        final DataBuffer dataBuffer = image.getRaster().getDataBuffer();
		        final byte layerBytes[] = ((DataBufferByte)dataBuffer).getData();
				System.arraycopy(layerBytes, 0, imageBytes, ix * xDim * yDim, xDim * yDim);
			}
	        
	        return imageBytes;

		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

    private static File checkFile(final String fileName) throws Exception {
    	final File imageFile = new File(fileName);
    	if (!imageFile.exists() || imageFile.isDirectory()) {
    		throw new Exception ("Image file does not exist: " + fileName);
    	}
    	return imageFile;
    }

    private static ImageReader buildReader(final File imageFile, final int zDim) throws Exception {
		final ImageInputStream imgInStream = ImageIO.createImageInputStream(imageFile);
		if (imgInStream == null || imgInStream.length() == 0){
			throw new Exception("Data load error - No input stream.");
		}
		Iterator<ImageReader> iter = ImageIO.getImageReaders(imgInStream);
		if (iter == null || !iter.hasNext()) {
			throw new Exception("Data load error - Image file format not supported by ImageIO.");
		}
		final ImageReader reader = iter.next();
		iter = null;
		reader.setInput(imgInStream);
		int numPages;
		if ((numPages = reader.getNumImages(true)) != zDim) {
			throw new Exception("Data load error - Number of pages mismatch: " + numPages + " expected: " + zDim);
		}
		return reader;
    }
}