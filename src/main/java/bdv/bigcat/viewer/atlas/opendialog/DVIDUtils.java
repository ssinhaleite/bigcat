package bdv.bigcat.viewer.atlas.opendialog;

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
import net.imglib2.img.basictypeaccess.array.ByteArray;
import net.imglib2.img.basictypeaccess.array.DoubleArray;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.img.basictypeaccess.array.IntArray;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.img.basictypeaccess.array.ShortArray;
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
	static DataType datatype;

	private DVIDUtils()
	{}

	@SuppressWarnings( { "unchecked", "rawtypes" } )
	public static final < T extends NativeType< T > > RandomAccessibleInterval< T > openVolatile(
			final String dvidURL,
			final String repoUUID,
			final String dataset,
			final double[] offset ) throws IOException
	{
		String infoUrl = dvidURL + "/" + repoUUID + "/" + dataset + "/info";
		final DVIDResponse response = DVIDParser.fetch( infoUrl, DVIDResponse.class );
		final int[] blockSize = new int[] { response.Extended.BlockSize[ 0 ], response.Extended.BlockSize[ 1 ], response.Extended.BlockSize[ 2 ] };

		String type = "";
		if ( response.Extended.Values.size() > 0 )
			type = response.Extended.Values.get( 0 ).DataType;

		DVIDUtils.datatype = DataType.fromString( type );

		// complete image size
		final long[] dimensions = new long[] {
				( long ) ( response.Extended.MaxPoint[ 0 ] - response.Extended.MinPoint[ 0 ] + 1 ),
				( long ) ( response.Extended.MaxPoint[ 1 ] - response.Extended.MinPoint[ 1 ] + 1 ),
				( long ) ( response.Extended.MaxPoint[ 2 ] - response.Extended.MinPoint[ 2 ] + 1 ) };

		final double[] dimensionsD = new double[] {
				( response.Extended.MaxPoint[ 0 ] - response.Extended.MinPoint[ 0 ] + 1 ),
				( response.Extended.MaxPoint[ 1 ] - response.Extended.MinPoint[ 1 ] + 1 ),
				( response.Extended.MaxPoint[ 2 ] - response.Extended.MinPoint[ 2 ] + 1 ) };

		final CellGrid grid = new CellGrid( dimensions, blockSize );

		final DVIDLoader< T > loader = new DVIDLoader<>( dvidURL, repoUUID, dataset, blockSize, datatype );

		final CachedCellImg< T, ? > img;
		final T finalType;
		final Cache< Long, Cell< ? > > cache;

		switch ( datatype )
		{
		case INT8:
			finalType = ( T ) new ByteType();
			cache = ( Cache ) new SoftRefLoaderCache< Long, Cell< ByteArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, finalType ) );
			img = new CachedCellImg( grid, finalType, cache, ArrayDataAccessFactory.get( BYTE ) );
			break;
		case UINT8:
			finalType = ( T ) new UnsignedByteType();
			cache = ( Cache ) new SoftRefLoaderCache< Long, Cell< ByteArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, finalType ) );
			img = new CachedCellImg( grid, finalType, cache, ArrayDataAccessFactory.get( BYTE ) );
			break;
		case INT16:
			finalType = ( T ) new ShortType();
			cache = ( Cache ) new SoftRefLoaderCache< Long, Cell< ShortArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, finalType ) );
			img = new CachedCellImg( grid, finalType, cache, ArrayDataAccessFactory.get( SHORT ) );
			break;
		case UINT16:
			finalType = ( T ) new UnsignedShortType();
			cache = ( Cache ) new SoftRefLoaderCache< Long, Cell< ShortArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, finalType ) );
			img = new CachedCellImg( grid, finalType, cache, ArrayDataAccessFactory.get( SHORT ) );
			break;
		case INT32:
			finalType = ( T ) new IntType();
			cache = ( Cache ) new SoftRefLoaderCache< Long, Cell< IntArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, finalType ) );
			img = new CachedCellImg( grid, finalType, cache, ArrayDataAccessFactory.get( INT ) );
			break;
		case UINT32:
			finalType = ( T ) new UnsignedIntType();
			cache = ( Cache ) new SoftRefLoaderCache< Long, Cell< IntArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, finalType ) );
			img = new CachedCellImg( grid, finalType, cache, ArrayDataAccessFactory.get( INT ) );
			break;
		case INT64:
			finalType = ( T ) new LongType();
			cache = ( Cache ) new SoftRefLoaderCache< Long, Cell< LongArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, finalType ) );
			img = new CachedCellImg( grid, finalType, cache, ArrayDataAccessFactory.get( LONG ) );
			break;
		case UINT64:
			finalType = ( T ) new UnsignedLongType();
			cache = ( Cache ) new SoftRefLoaderCache< Long, Cell< LongArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, finalType ) );
			img = new CachedCellImg( grid, finalType, cache, ArrayDataAccessFactory.get( LONG ) );
			break;
		case FLOAT32:
			finalType = ( T ) new FloatType();
			cache = ( Cache ) new SoftRefLoaderCache< Long, Cell< FloatArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, finalType ) );
			img = new CachedCellImg( grid, finalType, cache, ArrayDataAccessFactory.get( FLOAT ) );
			break;
		case FLOAT64:
			finalType = ( T ) new DoubleType();
			cache = ( Cache ) new SoftRefLoaderCache< Long, Cell< DoubleArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, finalType ) );
			img = new CachedCellImg( grid, finalType, cache, ArrayDataAccessFactory.get( DOUBLE ) );
			break;
		default:
			img = null;
		}

		return img;
	}

}
