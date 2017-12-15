package bdv.bigcat.viewer.atlas.opendialog;

import static net.imglib2.cache.img.DiskCachedCellImgOptions.options;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.LongFunction;

import org.apache.commons.io.IOUtils;

import net.imglib2.Cursor;
import net.imglib2.Interval;
import net.imglib2.RandomAccessible;
import net.imglib2.algorithm.gradient.PartialDerivative;
import net.imglib2.cache.img.CellLoader;
import net.imglib2.cache.img.DiskCachedCellImgFactory;
import net.imglib2.cache.img.DiskCachedCellImgOptions;
import net.imglib2.cache.img.DiskCachedCellImgOptions.CacheType;
import net.imglib2.cache.img.SingleCellArrayImg;
import net.imglib2.cache.util.IntervalKeyLoaderAsLongKeyLoader;
import net.imglib2.converter.Converters;
import net.imglib2.converter.RealFloatConverter;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.volatiles.array.DirtyVolatileByteArray;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;

public class DVIDCellLoader< T extends NativeType< T > > implements CellLoader< T >
{
	private final String dvid;

	private final String commit;

	private final String dataset;

	private final double[] offset;

	private final long[] dimensions;

	private final int[] blocksize;

	private final CellGrid grid;

	public DVIDCellLoader(
			final String dvid,
			final String commit,
			final String dataset,
			final double[] offset,
			final long[] dimensions,
			final int[] blocksize,
			final CellGrid grid ) throws IOException
	{
		super();
		this.dvid = dvid;
		this.commit = commit;
		this.dataset = dataset;
		this.offset = offset;
		this.dimensions = dimensions;
		this.blocksize = blocksize;
		this.grid = grid;

	}

	@Override
	public void load( SingleCellArrayImg< T, ? > cell ) throws Exception
	{
		// TODO: isotropic??
		final String format = String.format( "%s/%s/%s/%s/%s/%s",
				dvid, commit, dataset, "isotropic/0_1_2",
				"%d_%d_%d",
				"%d_%d_%d" );

		System.out.println( "format: " + format );

		final Function< Interval, String > addressComposer = interval -> {
			final String address = String.format(
					format,
					interval.max( 0 ) - interval.min( 0 ) + 1,
					interval.max( 1 ) - interval.min( 1 ) + 1,
					interval.max( 2 ) - interval.min( 2 ) + 1,
					offset[ 0 ] + interval.min( 0 ),
					offset[ 1 ] + interval.min( 1 ),
					offset[ 2 ] + interval.min( 2 ) );
			System.out.println( "address: " + address );
			return address;
		};

		final BiConsumer< byte[], DirtyVolatileByteArray > copier = ( bytes, access ) -> {
			System.arraycopy( bytes, 0, access.getCurrentStorageArray(), 0, bytes.length );
			access.setDirty();
		};

		final HTTPLoader< DirtyVolatileByteArray > functor = new HTTPLoader<>( addressComposer, ( n ) -> new DirtyVolatileByteArray( ( int ) n, true ), copier );
		final IntervalKeyLoaderAsLongKeyLoader< DirtyVolatileByteArray > loader = new IntervalKeyLoaderAsLongKeyLoader<>( grid, functor );

		final DiskCachedCellImgOptions factoryOptions = options()
				.cacheType( CacheType.BOUNDED )
				.maxCacheSize( 1000 )
				.cellDimensions( blocksize );

		final Img< UnsignedByteType > httpImg = new DiskCachedCellImgFactory< UnsignedByteType >( factoryOptions )
				.createWithCacheLoader( dimensions, new UnsignedByteType(), loader );

		final RandomAccessible< FloatType > source = Converters.convert( Views.extendBorder( httpImg ), new RealFloatConverter<>(), new FloatType() );
		final CellLoader< FloatType > gradientLoader = new CellLoader< FloatType >()
		{
			@Override
			public void load( final SingleCellArrayImg< FloatType, ? > cell ) throws Exception
			{
				final int n = cell.numDimensions();
				for ( int d = 0; d < n; ++d )
				{
					final Img< FloatType > imgDim = ArrayImgs.floats( Intervals.dimensionsAsLongArray( cell ) );
					PartialDerivative.gradientCentralDifference2( Views.offsetInterval( source, cell ), imgDim, d );
					final Cursor< FloatType > c = imgDim.cursor();
					for ( final FloatType t : cell )
					{
						final float val = c.next().get();
						t.set( t.get() + val * val );
					}
				}
				for ( final FloatType t : cell )
					t.set( ( float ) Math.sqrt( t.get() ) );
			}
		};

		final Img< FloatType > gradientImg = new DiskCachedCellImgFactory< FloatType >( factoryOptions )
				.create( dimensions, new FloatType(), gradientLoader,
						options().initializeCellsAsDirty( true ) );

//		final BdvSource httpSource = BdvFunctions.show(
//				VolatileViews.wrapAsVolatile( httpImg, new SharedQueue( 20 ) ),
//				"dvid" );
//
//		final int numProc = Runtime.getRuntime().availableProcessors();
//		final BdvSource gradientSource = BdvFunctions.show(
//				VolatileViews.wrapAsVolatile( gradientImg, new SharedQueue( numProc - 1 ) ),
//				"gradient",
//				BdvOptions.options().addTo( httpSource ) );
//
//		final Bdv bdv = httpSource;
//		bdv.getBdvHandle().getViewerPanel().setDisplayMode( SINGLE );
//		httpSource.setDisplayRange( 0.0, 255.0 );
//		gradientSource.setDisplayRange( 0.0, 30.0 );
	}

	public class HTTPLoader< A > implements Function< Interval, A >
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
