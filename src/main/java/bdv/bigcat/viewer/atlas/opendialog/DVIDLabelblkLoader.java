package bdv.bigcat.viewer.atlas.opendialog;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.LongArrayDataBlock;

import net.imglib2.type.NativeType;

public class DVIDLabelblkLoader< T extends NativeType< T > > extends DVIDLoader< T >
{
	public DVIDLabelblkLoader( String dvidURL, String repoUUID, String dataset, int[] blockSize, double[] voxelSize, double[] minPoint, DataType dataType )
	{
		super( dvidURL, repoUUID, dataset, blockSize, voxelSize, minPoint, dataType );
	}

	@Override
	protected String createURL()
	{
		// labelblk:
		// <api URL>/node/<UUID>/<data
		// name>/raw/<dims>/<size>/<offset>[/<format>][?queryopts]

		// Retrieves either 2d images (PNG by default) or 3d binary data,
		// depending on the dims parameter.
		// The 3d binary data response has "Content-type" set to
		// "application/octet-stream" and is an array of
		// voxel values in ZYX order (X iterates most rapidly).
		// size: Size in voxels along each dimension specified in <dims>.
		// offset: Gives coordinate of first voxel using dimensionality of data.

		final StringBuffer buf = new StringBuffer( dvidURL );

		buf.append( "/api/node/" );
		buf.append( repoUUID );
		buf.append( "/" );
		buf.append( dataset );
		buf.append( "/raw/0_1_2/" );
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

	@Override
	protected DataBlock< ? > readBlock( String urlString ) throws IOException
	{
		final long[] data = new long[ blockSize[ 0 ] * blockSize[ 1 ] * blockSize[ 2 ] ];
		final byte[] bytes = new byte[ data.length * 8 ];
		final URL url = new URL( urlString );
		final InputStream in = url.openStream();
		int off = 0, l = 0;
		do
		{
			l = in.read( bytes, off, bytes.length - off );
			off += l;
		}
		while ( l > 0 && off < data.length );
		in.close();

		for ( int i = 0, j = -1; i < data.length; ++i )
		{
			data[ i ] =
					( 0xffl & bytes[ ++j ] ) |
							( ( 0xffl & bytes[ ++j ] ) << 8 ) |
							( ( 0xffl & bytes[ ++j ] ) << 16 ) |
							( ( 0xffl & bytes[ ++j ] ) << 24 ) |
							( ( 0xffl & bytes[ ++j ] ) << 32 ) |
							( ( 0xffl & bytes[ ++j ] ) << 40 ) |
							( ( 0xffl & bytes[ ++j ] ) << 48 ) |
							( ( 0xffl & bytes[ ++j ] ) << 56 );
		}

		DataBlock< ? > dataBlock = new LongArrayDataBlock( blockSize, gridPosition, data );
		return dataBlock;
	}

	private String createTileURL( final long[] gridPosition )
	{
		// <api URL>/node/<UUID>/<data name>/blocks/<size>/<offset>
		// Error: io exception at url.openstream();
		// Zero bytes downloaded

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
		buf.append( "/blocks/" );
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

		// https://github.com/google/neuroglancer/blob/98ee36743205c0a0b9dc8785dab3a5126805129b/src/neuroglancer/datasource/dvid/backend.ts#L77-L88
		// Error: mal formed url
		// this URL is not listed on the help of the uint<>blk and neither on labelblk.

//		final StringBuffer buf = new StringBuffer( dvidURL );
//
//		buf.append( "/api/node/" );
//		buf.append( repoUUID );
//		buf.append( "/" );
//		buf.append( dataset );
//		buf.append( "/tile/0_1_2/" ); // 0_1_2 = dims
//		buf.append( "1/" ); // level
//		buf.append( gridPosition[ 0 ] );
//		buf.append( "_" );
//		buf.append( gridPosition[ 1 ] );
//		buf.append( "_" );
//		buf.append( gridPosition[ 2 ] );

		return buf.toString();
	}

}
