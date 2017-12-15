package bdv.bigcat.viewer.atlas.opendialog;

import static net.imglib2.cache.img.AccessFlags.VOLATILE;
import static net.imglib2.cache.img.PrimitiveType.BYTE;
import static net.imglib2.cache.img.PrimitiveType.DOUBLE;
import static net.imglib2.cache.img.PrimitiveType.FLOAT;
import static net.imglib2.cache.img.PrimitiveType.INT;
import static net.imglib2.cache.img.PrimitiveType.LONG;
import static net.imglib2.cache.img.PrimitiveType.SHORT;

import java.io.IOException;

import org.janelia.saalfeldlab.n5.DataType;

import net.imglib2.RandomAccessibleInterval;
import net.imglib2.cache.Cache;
import net.imglib2.cache.img.ArrayDataAccessFactory;
import net.imglib2.cache.img.CachedCellImg;
import net.imglib2.cache.img.LoadedCellCacheLoader;
import net.imglib2.cache.ref.SoftRefLoaderCache;
import net.imglib2.img.basictypeaccess.volatiles.array.VolatileByteArray;
import net.imglib2.img.basictypeaccess.volatiles.array.VolatileDoubleArray;
import net.imglib2.img.basictypeaccess.volatiles.array.VolatileFloatArray;
import net.imglib2.img.basictypeaccess.volatiles.array.VolatileIntArray;
import net.imglib2.img.basictypeaccess.volatiles.array.VolatileLongArray;
import net.imglib2.img.basictypeaccess.volatiles.array.VolatileShortArray;
import net.imglib2.img.cell.Cell;
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

public class DVIDUtils
{
	private DVIDUtils()
	{}

	@SuppressWarnings( { "unchecked", "rawtypes" } )
	public static final < T extends NativeType< T > > RandomAccessibleInterval< T > openVolatile(
			final String dvidURL,
			final String commit,
			final String dataset,
			final double[] offset ) throws IOException
	{
		// TODO
		final long[] dimensions = new long[] { 300, 300, 300 };
		final int[] blockSize = new int[] { 64, 64, 64 };
		DataType datatype = DataType.INT8;

		final CellGrid grid = new CellGrid( dimensions, blockSize );
		final DVIDCellLoader< T > loader = new DVIDCellLoader<>( dvidURL, commit, dataset, offset, dimensions, blockSize, grid );

		final CachedCellImg< T, ? > img;
		final T type;
		final Cache< Long, Cell< ? > > cache;

		switch ( datatype )
		{
		case INT8:
			type = ( T ) new ByteType();
			cache = ( Cache ) new SoftRefLoaderCache< Long, Cell< VolatileByteArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, type, VOLATILE ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( BYTE, VOLATILE ) );
			break;
		case UINT8:
			type = ( T ) new UnsignedByteType();
			cache = ( Cache ) new SoftRefLoaderCache< Long, Cell< VolatileByteArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, type, VOLATILE ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( BYTE, VOLATILE ) );
			break;
		case INT16:
			type = ( T ) new ShortType();
			cache = ( Cache ) new SoftRefLoaderCache< Long, Cell< VolatileShortArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, type, VOLATILE ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( SHORT, VOLATILE ) );
			break;
		case UINT16:
			type = ( T ) new UnsignedShortType();
			cache = ( Cache ) new SoftRefLoaderCache< Long, Cell< VolatileShortArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, type, VOLATILE ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( SHORT, VOLATILE ) );
			break;
		case INT32:
			type = ( T ) new IntType();
			cache = ( Cache ) new SoftRefLoaderCache< Long, Cell< VolatileIntArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, type, VOLATILE ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( INT, VOLATILE ) );
			break;
		case UINT32:
			type = ( T ) new UnsignedIntType();
			cache = ( Cache ) new SoftRefLoaderCache< Long, Cell< VolatileIntArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, type, VOLATILE ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( INT, VOLATILE ) );
			break;
		case INT64:
			type = ( T ) new LongType();
			cache = ( Cache ) new SoftRefLoaderCache< Long, Cell< VolatileLongArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, type, VOLATILE ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( LONG, VOLATILE ) );
			break;
		case UINT64:
			type = ( T ) new UnsignedLongType();
			cache = ( Cache ) new SoftRefLoaderCache< Long, Cell< VolatileLongArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, type, VOLATILE ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( LONG, VOLATILE ) );
			break;
		case FLOAT32:
			type = ( T ) new FloatType();
			cache = ( Cache ) new SoftRefLoaderCache< Long, Cell< VolatileFloatArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, type, VOLATILE ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( FLOAT, VOLATILE ) );
			break;
		case FLOAT64:
			type = ( T ) new DoubleType();
			cache = ( Cache ) new SoftRefLoaderCache< Long, Cell< VolatileDoubleArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, type, VOLATILE ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( DOUBLE, VOLATILE ) );
			break;
		default:
			img = null;
		}

		return img;
	}
}
