package fileLoader;

import java.awt.image.BufferedImage;
import java.awt.image.DataBuffer;
import java.awt.image.DataBufferByte;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Iterator;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;

public class LoadMultiStack
{

    public static byte[] load(String fileName, int xDim, int yDim, int zDim) {

		try {
			final File imageFile = checkFile(fileName);		
			final ImageReader reader = buildReader(imageFile, zDim);
	        final byte imageBytes[] = new byte[xDim * yDim * zDim];
	        
//			try (DataInputStream input = new DataInputStream(new FileInputStream(imageFile))) {
//			    input.readFully(imageBytes);
//			}

			for (int ix = 0; ix < zDim; ix++) {
				final BufferedImage image = reader.read(ix);
		        final DataBuffer dataBuffer = image.getRaster().getDataBuffer();
		        final byte layerBytes[] = ((DataBufferByte)dataBuffer).getData();
				System.out.println(layerBytes.length + "    " + xDim * yDim);
				System.arraycopy(layerBytes, 0, imageBytes, ix * xDim * yDim, xDim * yDim);
			}

	        for (int iz = 0 ; iz < zDim ; iz++) {
	        	System.out.println("--------------------------------------");
		        for (int iy = 0 ; iy < yDim ; iy++) {
		        	System.out.println();
			        for (int ix = 0 ; ix < xDim ; ix++) {
			        	System.out.print(imageBytes[iz * yDim * xDim + iy * xDim + ix] + " ");
			        }
		        }
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