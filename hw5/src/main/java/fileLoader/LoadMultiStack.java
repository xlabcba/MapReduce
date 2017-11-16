package fileLoader;

import java.awt.image.BufferedImage;
import java.awt.image.DataBuffer;
import java.awt.image.DataBufferByte;
import java.awt.image.WritableRaster;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class LoadMultiStack
{
	
	private static AmazonS3 s3Client;
	
	static {
	    s3Client = new AmazonS3Client();  
	}
	
    public static void saveImages(byte[][][] matrix, String bucketName, String filePath) {

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
				String fileName = filePath + "_layer" + iz + ".tiff";
				if (bucketName.isEmpty()) {
					saveFileLocal(fileName, b);
				} else {
					saveFileRemote(bucketName, fileName, b, xDim, yDim);
				}		
				System.out.println(fileName + "saved");
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
    
    private static void saveFileLocal(final String fileName, final BufferedImage b) throws Exception {
		File file = new File(fileName);
		file.getParentFile().mkdirs();
		ImageIO.write(b, "TIFF", new File(fileName));		
    }
    
    private static void saveFileRemote(final String bucketName, final String fileName, 
    		final BufferedImage b, final int xDim, final int yDim) throws Exception {
		ByteArrayOutputStream imageData = new ByteArrayOutputStream(xDim * yDim);
		ImageIO.write(b, "TIFF", imageData);
		byte[] byteArray = imageData.toByteArray();
	    InputStream stream = new ByteArrayInputStream(byteArray);    
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(byteArray.length);
        meta.setContentType("image/tiff");
        s3Client.putObject(new PutObjectRequest(bucketName, fileName, stream, meta));
    }

    public static byte[][] loadImage(String bucketName, String fileName, int xDim, int yDim, int zDim) {

		try {
			final InputStream imageData;
			if (bucketName.isEmpty()) {
				imageData = checkFileLocal(fileName);
			} else {
				imageData = checkFileRemote(bucketName, fileName);
			}
			final ImageReader reader = buildReader(imageData, zDim);
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

    private static InputStream checkFileLocal(final String fileName) throws Exception {
    	final File imageFile = new File(fileName);
    	if (!imageFile.exists() || imageFile.isDirectory()) {
    		throw new Exception ("Image file does not exist: " + fileName);
    	}
    	return new FileInputStream(imageFile);
    }
    
    private static InputStream checkFileRemote(final String bucketName, final String fileName) throws Exception {
    	S3Object image = s3Client.getObject(new GetObjectRequest(bucketName, fileName));
    	InputStream imageData = image.getObjectContent();
    	return imageData;
    }

    private static ImageReader buildReader(final InputStream imageData, final int zDim) throws Exception {
		final ImageInputStream imgInStream = ImageIO.createImageInputStream(imageData);
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