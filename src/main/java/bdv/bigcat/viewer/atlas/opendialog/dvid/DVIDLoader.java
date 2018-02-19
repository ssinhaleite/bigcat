package bdv.bigcat.viewer.atlas.opendialog.dvid;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.function.BiConsumer;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public abstract class DVIDLoader< T extends NativeType< T > > implements CellLoader< T >
{
	private static final Logger LOG = LoggerFactory.getLogger( MethodHandles.lookup().lookupClass() );

	protected final String dvidURL;

	protected final String repoUUID;

	protected final String dataset;

	protected final int[] blockSize;

	protected final double[] minPoint;

	protected long[] gridPosition;

	protected long[] offset;

	protected long[] cellDimensions;

	protected long[] cellMin;

	protected final BiConsumer< Img< T >, DataBlock< ? > > copyFromBlock;

	public DVIDLoader(
			final String dvidURL,
			final String repoUUID,
			final String dataset,
			final int[] blockSize,
			final double[] voxelSize,
			final double[] minPoint,
			final DataType dataType )
	{
		this.dvidURL = dvidURL;
		this.repoUUID = repoUUID;
		this.dataset = dataset;
		this.blockSize = blockSize;
		this.minPoint = minPoint;
		this.copyFromBlock = createCopy( dataType );
	}

	protected abstract String createURL();

	protected abstract DataBlock< ? > readBlock(
			final String urlString ) throws IOException;

	@Override
	public void load( final SingleCellArrayImg< T, ? > cell )
	{
		gridPosition = new long[ cell.numDimensions() ];
		offset = new long[ cell.numDimensions() ];
		for ( int d = 0; d < gridPosition.length; ++d )
		{
			gridPosition[ d ] = cell.min( d ) / blockSize[ d ];
			offset[ d ] = ( long ) ( cell.min( d ) + minPoint[ d ] );
		}

		final DataBlock< ? > block;
		try
		{
			final String urlString = createURL();
			block = readBlock( urlString );

			LOG.debug( "{} {}", urlString, block.getNumElements() );
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
