package bdv.bigcat.viewer.atlas.opendialog;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.LongFunction;

import org.apache.commons.io.IOUtils;
import org.janelia.saalfeldlab.n5.DataType;

import net.imglib2.Interval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.cache.img.CachedCellImg;
import net.imglib2.cache.img.DiskCachedCellImgFactory;
import net.imglib2.cache.img.DiskCachedCellImgOptions;
import net.imglib2.cache.img.DiskCachedCellImgOptions.CacheType;
import net.imglib2.cache.util.IntervalKeyLoaderAsLongKeyLoader;
import net.imglib2.img.basictypeaccess.volatiles.array.DirtyVolatileByteArray;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.integer.ByteType;
import net.imglib2.type.numeric.integer.IntType;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.type.numeric.integer.ShortType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedIntType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;

public class DVIDUtils
{
	static DataType datatype;

	private DVIDUtils()
	{}

	@SuppressWarnings( { "unchecked" } )
	public static final < T extends NativeType< T > > RandomAccessibleInterval< T > openVolatile(
			final String dvidURL,
			final String repoUUID,
			final String dataset,
			final double[] offset ) throws IOException
	{
		String infoUrl = dvidURL + "/" + repoUUID + "/" + dataset + "/info";
		final DVIDResponse response = DVIDParser.fetch( infoUrl, DVIDResponse.class );
		final int[] blockSize = new int[] { response.Extended.BlockSize[ 0 ], response.Extended.BlockSize[ 1 ], response.Extended.BlockSize[ 2 ] };

		// TODO: values.get(0)?
		String type = response.Extended.Values.get( 0 ).DataType;
		DVIDUtils.datatype = DataType.fromString( type );

		// image size
		final long[] dimensions = new long[] { response.Extended.MaxPoint[ 0 ] - response.Extended.MinPoint[ 0 ], response.Extended.MaxPoint[ 1 ] - response.Extended.MinPoint[ 1 ], response.Extended.MaxPoint[ 2 ] - response.Extended.MinPoint[ 2 ] };

		final CellGrid grid = new CellGrid( dimensions, blockSize );
		final BiConsumer< byte[], DirtyVolatileByteArray > copier = ( bytes, access ) -> {
			System.arraycopy( bytes, 0, access.getCurrentStorageArray(), 0, bytes.length );
			access.setDirty();
		};

		// TODO: isotropic/0/1/2 ?
		final String format = String.format( "%s/%s/%s/%s/%s/%s",
				dvidURL, repoUUID, dataset, "isotropic/0_1_2",
				"%d_%d_%d",
				"%d_%d_%d" );

		Function< Interval, String > addressComposer = interval -> {
			final String address = String.format(
					format,
					( int ) interval.max( 0 ) - interval.min( 0 ) + 1,
					( int ) interval.max( 1 ) - interval.min( 1 ) + 1,
					( int ) interval.max( 2 ) - interval.min( 2 ) + 1,
					( int ) offset[ 0 ] + interval.min( 0 ),
					( int ) offset[ 1 ] + interval.min( 1 ),
					( int ) offset[ 2 ] + interval.min( 2 ) );
			return address;
		};

		final HTTPLoader< DirtyVolatileByteArray > functor = new HTTPLoader<>( addressComposer, ( n ) -> new DirtyVolatileByteArray( ( int ) n, true ), copier );

		final IntervalKeyLoaderAsLongKeyLoader< DirtyVolatileByteArray > loader = new IntervalKeyLoaderAsLongKeyLoader<>( grid, functor );

		final DiskCachedCellImgOptions factoryOptions = DiskCachedCellImgOptions.options()
				.cacheType( CacheType.BOUNDED )
				.maxCacheSize( 1000 )
				.cellDimensions( blockSize );

		CachedCellImg< T, ? > img;

		switch ( datatype )
		{
		case INT8:
			img = ( CachedCellImg< T, ? > ) new DiskCachedCellImgFactory< ByteType >( factoryOptions )
					.createWithCacheLoader( dimensions, new ByteType(), loader );
			break;
		case UINT8:
			img = ( CachedCellImg< T, ? > ) new DiskCachedCellImgFactory< UnsignedByteType >( factoryOptions )
					.createWithCacheLoader( dimensions, new UnsignedByteType(), loader );
			break;
		case INT16:
			img = ( CachedCellImg< T, ? > ) new DiskCachedCellImgFactory< ShortType >( factoryOptions )
					.createWithCacheLoader( dimensions, new ShortType(), loader );
			break;
		case UINT16:
			img = ( CachedCellImg< T, ? > ) new DiskCachedCellImgFactory< UnsignedShortType >( factoryOptions )
					.createWithCacheLoader( dimensions, new UnsignedShortType(), loader );
			break;
		case INT32:
			img = ( CachedCellImg< T, ? > ) new DiskCachedCellImgFactory< IntType >( factoryOptions )
					.createWithCacheLoader( dimensions, new IntType(), loader );
			break;
		case UINT32:
			img = ( CachedCellImg< T, ? > ) new DiskCachedCellImgFactory< UnsignedIntType >( factoryOptions )
					.createWithCacheLoader( dimensions, new UnsignedIntType(), loader );
			break;
		case INT64:
			img = ( CachedCellImg< T, ? > ) new DiskCachedCellImgFactory< LongType >( factoryOptions )
					.createWithCacheLoader( dimensions, new LongType(), loader );
			break;
		case UINT64:
			img = ( CachedCellImg< T, ? > ) new DiskCachedCellImgFactory< UnsignedLongType >( factoryOptions )
					.createWithCacheLoader( dimensions, new UnsignedLongType(), loader );
			break;
		case FLOAT32:
			img = ( CachedCellImg< T, ? > ) new DiskCachedCellImgFactory< FloatType >( factoryOptions )
					.createWithCacheLoader( dimensions, new FloatType(), loader );
			break;
		case FLOAT64:
			img = ( CachedCellImg< T, ? > ) new DiskCachedCellImgFactory< DoubleType >( factoryOptions )
					.createWithCacheLoader( dimensions, new DoubleType(), loader );
			break;
		default:
			img = null;
		}

		System.out.println( "img " + img );
		return img;
	}

	public static class HTTPLoader< A > implements Function< Interval, A >
	{

		private final Function< Interval, String > addressComposer;

		private final LongFunction< A > accessFactory;

		private final BiConsumer< byte[], A > copyToAccess;

		public HTTPLoader(
				final Function< Interval, String > addressComposer,
				final LongFunction< A > accessFactory,
				final BiConsumer< byte[], A > copyToAccess )
		{
			super();
			this.addressComposer = addressComposer;
			this.accessFactory = accessFactory;
			this.copyToAccess = copyToAccess;
		}

		@Override
		public A apply( final Interval interval )
		{
			try
			{
				final String address = addressComposer.apply( interval );
				final URL url = new URL( address );
				final InputStream stream = url.openStream();
				final long numElements = Intervals.numElements( interval );
				final byte[] response = IOUtils.toByteArray( stream );

				final A access = accessFactory.apply( numElements );
				copyToAccess.accept( response, access );

				return access;
			}
			catch ( final Exception e )
			{
				throw new RuntimeException( e );
			}

		}

	}
}
