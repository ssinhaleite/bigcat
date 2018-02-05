package bdv.bigcat.viewer.atlas.opendialog;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.function.BiConsumer;

import org.janelia.saalfeldlab.n5.ByteArrayDataBlock;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;

import net.imglib2.Cursor;
import net.imglib2.cache.img.CellLoader;
import net.imglib2.cache.img.SingleCellArrayImg;
import net.imglib2.img.Img;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.integer.GenericByteType;
import net.imglib2.type.numeric.integer.GenericIntType;
import net.imglib2.type.numeric.integer.GenericLongType;
import net.imglib2.type.numeric.integer.GenericShortType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.type.numeric.real.FloatType;

public class DVIDLoader< T extends NativeType< T > > implements CellLoader< T >
{
	private final String dvidURL;

	private final String repoUUID;

	private final String dataset;

	private final int[] blockSize;

	private final double[] minPoint;

	private final boolean isRaw;

	private final BiConsumer< Img< T >, DataBlock< ? > > copyFromBlock;

	public DVIDLoader(
			final String dvidURL,
			final String repoUUID,
			final String dataset,
			final int[] blockSize,
			final double[] voxelSize,
			final double[] minPoint,
			final DataType dataType,
			final boolean isRaw )
	{
		this.dvidURL = dvidURL;
		this.repoUUID = repoUUID;
		this.dataset = dataset;
		this.blockSize = blockSize;
		this.minPoint = minPoint;
		this.copyFromBlock = createCopy( dataType );
		this.isRaw = isRaw;
	}

	private DataBlock< ? > readBlock(
			final String urlString,
			final long[] gridPosition,
			final byte[] data ) throws IOException
	{
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

	private String createRawBlocksURL( final long[] gridPosition )
	{
		// <api URL>/node/<UUID>/<data name>/blocks/<block coord>/<spanX>
		// Retrieves "spanX" blocks of uncompressed voxel data along X starting
		// from given block coordinate.
		final StringBuffer buf = new StringBuffer( dvidURL );

		buf.append( "/" );
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

	private String createTileURL( final long[] gridPosition )
	{
		// https://github.com/google/neuroglancer/blob/98ee36743205c0a0b9dc8785dab3a5126805129b/src/neuroglancer/datasource/dvid/backend.ts#L77-L88
		// this URL is not listed on the help of the uint<>blk and labelblk.

		final StringBuffer buf = new StringBuffer( dvidURL );

		buf.append( "/" );
		buf.append( repoUUID );
		buf.append( "/" );
		buf.append( dataset );
		buf.append( "/tile/0_1_2/" ); // 0_1_2 = dims
		buf.append( "1/" ); // level
		buf.append( gridPosition[ 0 ] );
		buf.append( "_" );
		buf.append( gridPosition[ 1 ] );
		buf.append( "_" );
		buf.append( gridPosition[ 2 ] );

		return buf.toString();
	}

	private String createRawURL( final long[] offset )
	{
		// <api URL>/node/<UUID>/<data name>/raw/<dims>/<size>/<offset>
		// Retrieves either 2d images (PNG by default) or 3d binary data,
		// depending on the dims parameter.
		// The 3d binary data response has "Content-type" set to
		// "application/octet-stream" and is an array of voxel values in ZYX
		// order (X iterates most rapidly).
		// If the underlying data is float32, then the little-endian four byte
		// format is written as RGBA.

		final StringBuffer buf = new StringBuffer( dvidURL );

		buf.append( "/" );
		buf.append( repoUUID );
		buf.append( "/" );
		buf.append( dataset );
		buf.append( "/raw/0_1_2/" ); // 0_1_2 = dims
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

	private String createURL( final long[] offset )
	{
		// labelblk:
		// <api URL>/node/<UUID>/<data name>/blocks/<size>/<offset>

		// uint:
		// <api URL>/node/<UUID>/<data name>/subvolblocks/<size>/<offset>
		// Expected internal block data to be JPEG, was LZ4 compression instead

		// Retrieves blocks corresponding to the extents specified by the size
		// and offset. The subvolume request must be block aligned. This is the
		// most server-efficient way of retrieving labelblk data, where data
		// read from the underlying storage engine is written directly to the
		// HTTP connection.
		// size: Size in voxels along each dimension specified in <dims>.
		// offset: Gives coordinate of first voxel using dimensionality of data.

		final StringBuffer buf = new StringBuffer( dvidURL );

		buf.append( "/" );
		buf.append( repoUUID );
		buf.append( "/" );
		buf.append( dataset );

		if ( isRaw )
			buf.append( "/subvolblocks/" );
		else
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

		return buf.toString();
	}

	@Override
	public void load( final SingleCellArrayImg< T, ? > cell )
	{
		final long[] gridPosition = new long[ cell.numDimensions() ];
		final long[] offset = new long[ cell.numDimensions() ];
		for ( int d = 0; d < gridPosition.length; ++d )
		{
			gridPosition[ d ] = cell.min( d ) / blockSize[ d ];
			offset[ d ] = ( long ) ( cell.min( d ) + minPoint[ d ] );
		}

		final DataBlock< ? > block;
		final byte[] data = new byte[ blockSize[ 0 ] * blockSize[ 1 ] * blockSize[ 2 ] ];

		try
		{
			final String urlString;
			if (isRaw)
				urlString = createRawBlocksURL( gridPosition );
			else
				urlString = createRawURL( offset );

			block = readBlock( urlString, gridPosition, data );
		}
		catch ( final IOException e )
		{
			throw new RuntimeException( e );
		}

		if ( block != null )
			copyFromBlock.accept( cell, block );
	}

	public static < T extends NativeType< T > > BiConsumer< Img< T >, DataBlock< ? > > createCopy( final DataType dataType )
	{
		switch ( dataType )
		{
		case INT8:
		case UINT8:
			return ( a, b ) -> {
				final byte[] data = ( byte[] ) b.getData();
				@SuppressWarnings( "unchecked" )
				final Cursor< ? extends GenericByteType< ? > > c = ( Cursor< ? extends GenericByteType< ? > > ) a.cursor();
				for ( int i = 0; i < data.length; ++i )
					c.next().setByte( data[ i ] );
			};
		case INT16:
		case UINT16:
			return ( a, b ) -> {
				final short[] data = ( short[] ) b.getData();
				@SuppressWarnings( "unchecked" )
				final Cursor< ? extends GenericShortType< ? > > c = ( Cursor< ? extends GenericShortType< ? > > ) a.cursor();
				for ( int i = 0; i < data.length; ++i )
					c.next().setShort( data[ i ] );
			};
		case INT32:
		case UINT32:
			return ( a, b ) -> {
				final int[] data = ( int[] ) b.getData();
				@SuppressWarnings( "unchecked" )
				final Cursor< ? extends GenericIntType< ? > > c = ( Cursor< ? extends GenericIntType< ? > > ) a.cursor();
				for ( int i = 0; i < data.length; ++i )
					c.next().setInt( data[ i ] );
			};
		case INT64:
		case UINT64:
			return ( a, b ) -> {
				System.out.println( "tyeste" );
				final long[] data = ( long[] ) b.getData();
				System.out.println( "data: " + data );
				@SuppressWarnings( "unchecked" )
				final Cursor< ? extends GenericLongType< ? > > c = ( Cursor< ? extends GenericLongType< ? > > ) a.cursor();
				for ( int i = 0; i < data.length; ++i )
					c.next().setLong( data[ i ] );
			};
		case FLOAT32:
			return ( a, b ) -> {
				final float[] data = ( float[] ) b.getData();
				@SuppressWarnings( "unchecked" )
				final Cursor< ? extends FloatType > c = ( Cursor< ? extends FloatType > ) a.cursor();
				for ( int i = 0; i < data.length; ++i )
					c.next().set( data[ i ] );
			};
		case FLOAT64:
			return ( a, b ) -> {
				final double[] data = ( double[] ) b.getData();
				@SuppressWarnings( "unchecked" )
				final Cursor< ? extends DoubleType > c = ( Cursor< ? extends DoubleType > ) a.cursor();
				for ( int i = 0; i < data.length; ++i )
					c.next().set( data[ i ] );
			};
		default:
			throw new IllegalArgumentException( "Type " + dataType.name() + " not supported!" );
		}
	}
}
