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

	private final BiConsumer< Img< T >, DataBlock< ? > > copyFromBlock;

	public DVIDLoader(
			final String dvidURL,
			final String repoUUID,
			final String dataset,
			final int[] blockSize,
			final DataType dataType )
	{
		this.dvidURL = dvidURL;
		this.repoUUID = repoUUID;
		this.dataset = dataset;
		this.blockSize = blockSize;
		this.copyFromBlock = createCopy( dataType );
	}

	private DataBlock< ? > readBlock(
			final long[] gridPosition,
			final int[] blockDimension ) throws IOException
	{
		final String urlString = createRawURL( gridPosition );

		final URL url = new URL( urlString );
		final byte[] data = new byte[ blockSize[ 0 ] * blockSize[ 1 ] * blockSize[ 2 ] ];
		final InputStream in = url.openStream();
		int off = 0, l = 0;
		do
		{
			l = in.read( data, off, data.length - off );
			off += l;
		}
		while ( l > 0 && off < data.length );
		in.close();

		DataBlock< ? > dataBlock = new ByteArrayDataBlock( blockDimension, gridPosition, data );
		return dataBlock;
	}

	private String createRawURL( final long[] gridPosition )
	{
		// <api URL>/node/<UUID>/<data name>/blocks/<block coord>/<spanX>
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

	private String createLabelURL( final long[] gridPosition )
	{

		// TODO:
		// Labelblk:
		// <api URL>/node/<UUID>/<data name>/blocks/<size>/<offset>
		// uint:
		// <api URL>/node/<UUID>/<data name>/subvolblocks/<size>/<offset>

		final StringBuffer buf = new StringBuffer( dvidURL );

		buf.append( "/" );
		buf.append( repoUUID );
		buf.append( "/" );
		buf.append( dataset );
		buf.append( "/subvolblocks/" );
//		buf.append( gridPosition[ 0 ] );
		buf.append( blockSize[ 0 ] );
		buf.append( "_" );
//		buf.append( gridPosition[ 1 ] );
		buf.append( blockSize[ 1 ] );
		buf.append( "_" );
//		buf.append( gridPosition[ 2 ] );
		buf.append( blockSize[ 2 ] );
//		buf.append( "/1" );
		buf.append( "/" );
		buf.append( gridPosition[ 0 ] );
		buf.append( "_" );
		buf.append( gridPosition[ 1 ] );
		buf.append( "_" );
		buf.append( gridPosition[ 2 ] );

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
//			offset[ d ] = cell.min( d ) + minPoint;

		}
		final DataBlock< ? > block;
		try
		{
			block = readBlock( gridPosition, blockSize );
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
				final long[] data = ( long[] ) b.getData();
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
