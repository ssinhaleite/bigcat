package bdv.bigcat.viewer.atlas.opendialog.dvid;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.janelia.saalfeldlab.n5.ByteArrayDataBlock;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;

import net.imglib2.type.NativeType;

public class DVIDUintblkLoader< T extends NativeType< T > > extends DVIDLoader< T >
{

	public DVIDUintblkLoader( String dvidURL, String repoUUID, String dataset, int[] blockSize, double[] voxelSize, double[] minPoint, DataType dataType )
	{
		super( dvidURL, repoUUID, dataset, blockSize, voxelSize, minPoint, dataType );
	}

	@Override
	protected String createURL()
	{
		// <api URL>/node/<UUID>/<data name>/blocks/<block coord>/<spanX>
		// Retrieves "spanX" blocks of uncompressed voxel data along X starting
		// from given block coordinate.
		final StringBuffer buf = new StringBuffer( dvidURL );

		buf.append( "/api/node/" );
		buf.append( repoUUID );
		buf.append( "/" );
		buf.append( dataset );
		buf.append( "/blocks/" );
		buf.append( gridPosition[ 0 ] );
		buf.append( "_" );
		buf.append( gridPosition[ 1 ] );
		buf.append( "_" );
		buf.append( gridPosition[ 2 ] );
		buf.append( "/1" );

		return buf.toString();
	}

	@Override
	protected DataBlock< ? > readBlock( String urlString ) throws IOException
	{
		final byte[] data = new byte[ blockSize[ 0 ] * blockSize[ 1 ] * blockSize[ 2 ] ];
		final URL url = new URL( urlString );
		final InputStream in = url.openStream();
		int off = 0, l = 0;
		do
		{
			l = in.read( data, off, data.length - off );
			off += l;
		}
		while ( l > 0 && off < data.length );
		in.close();

		DataBlock< ? > dataBlock = new ByteArrayDataBlock( blockSize, gridPosition, data );
		return dataBlock;
	}

	private String createRawURL( final long[] offset )
	{
		// uint:
		// <api URL>/node/<UUID>/<data name>/subvolblocks/<size>/<offset>
		// Error message: Expected internal block data to be JPEG, was LZ4
		// compression instead

		// Retrieves blocks corresponding to the extents specified by the size
		// and offset. The subvolume request must be block aligned. This is the
		// most server-efficient way of retrieving labelblk data, where data
		// read from the underlying storage engine is written directly to the
		// HTTP connection.
		// size: Size in voxels along each dimension specified in <dims>.
		// offset: Gives coordinate of first voxel using dimensionality of data.

		final StringBuffer buf = new StringBuffer( dvidURL );

		buf.append( "/api/node/" );
		buf.append( repoUUID );
		buf.append( "/" );
		buf.append( dataset );
		buf.append( "/subvolblocks/" );
		buf.append( blockSize[ 0 ] );
		buf.append( "_" );
		buf.append( blockSize[ 1 ] );
		buf.append( "_" );
		buf.append( blockSize[ 2 ] );
		buf.append( "/" );
		buf.append( offset[ 0 ] );
		buf.append( "_" );
		buf.append( offset[ 1 ] );
		buf.append( "_" );
		buf.append( offset[ 2 ] );

		return buf.toString();
	}

}
