package bdv.bigcat.viewer.atlas.opendialog;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.DoubleStream;

import bdv.img.h5.H5Utils;
import bdv.util.volatiles.SharedQueue;
import bdv.util.volatiles.VolatileViews;
import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.IHDF5Reader;
import javafx.stage.FileChooser;
import javafx.stage.FileChooser.ExtensionFilter;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.Volatile;
import net.imglib2.cache.volatiles.CacheHints;
import net.imglib2.cache.volatiles.LoadingStrategy;
import net.imglib2.type.NativeType;
import net.imglib2.util.Pair;
import net.imglib2.util.ValuePair;

public class BackendDialogHDF5 extends BackendDialogGroupAndDataset implements CombinesErrorMessages
{

	private static final String RESOLUTION_KEY = "resolution";

	private static final String OFFSET_KEY = "offset";

	private static final String MIN_KEY = "min";

	private static final String MAX_KEY = "max";

	private static final String AXIS_ORDER_KEY = "axisOrder";

	public BackendDialogHDF5()
	{
		super( "HDF file", "Dataset", ( group, scene ) -> {
			final FileChooser fileChooser = new FileChooser();
//			fileChooser.setInitialDirectory( new File( group ) );
			fileChooser.getExtensionFilters().addAll( new ExtensionFilter( "HDF5 Files", "*.hdf", "*.h5" ) );
			final File file = fileChooser.showOpenDialog( scene.getWindow() );
			return file.getAbsolutePath();
		} );
	}

	private static < T > boolean isLabelType( final Class< T > clazz, final boolean signed )
	{
		System.out.println( "IS LABEL TYPE ? " + clazz + " " + signed + " " + isIntegerType( clazz, signed ) + clazz.isAssignableFrom( long.class ) );
		return isLabelMultisetType( clazz ) || isIntegerType( clazz, signed );
	}

	private static < T > boolean isLabelMultisetType( final Class< T > clazz )
	{
		return false;
	}

	private static < T > boolean isIntegerType( final Class< T > clazz, final boolean signed )
	{
		if ( clazz.isAssignableFrom( byte.class ) || clazz.isAssignableFrom( short.class ) || clazz.isAssignableFrom( int.class ) || clazz.isAssignableFrom( long.class ) )
			return true;
		return false;
	}

	private static < T > double minForType( final Class< T > clazz, final boolean signed )
	{
		// TODO ever return non-zero here?
		return 0.0;
	}

	private static < T > double maxForType( final Class< T > clazz, final boolean signed )
	{
		if ( clazz.isAssignableFrom( byte.class ) )
			return signed ? Byte.MAX_VALUE : 0xff;
		if ( clazz.isAssignableFrom( short.class ) )
			return signed ? Short.MAX_VALUE : 0xffff;
		if ( clazz.isAssignableFrom( int.class ) )
			return signed ? Integer.MAX_VALUE : 0xffffffffl;
		if ( clazz.isAssignableFrom( long.class ) )
			return signed ? Long.MAX_VALUE : 2.0 * Long.MAX_VALUE;
		return 1.0;
	}

	@SuppressWarnings( "unchecked" )
	@Override
	public < T extends NativeType< T >, V extends Volatile< T > > Pair< RandomAccessibleInterval< T >[], RandomAccessibleInterval< V >[] > getDataAndVolatile(
			final SharedQueue sharedQueue,
			final int priority ) throws IOException
	{
		final String group = groupProperty.get();
		final IHDF5Reader reader = HDF5Factory.openForReading( group );
		final String dataset = this.dataset.get();
		// TODO optimize block size
		final RandomAccessibleInterval< T > raw = H5Utils.open( reader, dataset );
		final RandomAccessibleInterval< V > vraw = VolatileViews.wrapAsVolatile( raw, sharedQueue, new CacheHints( LoadingStrategy.VOLATILE, priority, true ) );
		return new ValuePair<>( new RandomAccessibleInterval[] { raw }, new RandomAccessibleInterval[] { vraw } );
	}

	@Override
	public boolean isLabelType() throws IOException
	{
		try (final IHDF5Reader reader = HDF5Factory.openForReading( groupProperty.get() ))
		{
			final Class< ? > dataType = reader.getDataSetInformation( dataset.get() ).getTypeInformation().tryGetJavaType();
			final boolean signed = reader.getDataSetInformation( dataset.get() ).getTypeInformation().isSigned();
			return isLabelType( dataType, signed );
		}
	}

	@Override
	public boolean isLabelMultisetType() throws IOException
	{
		try (final IHDF5Reader reader = HDF5Factory.openForReading( groupProperty.get() ))
		{
			final Class< ? > dataType = reader.getDataSetInformation( dataset.get() ).getTypeInformation().tryGetJavaType();
			return isLabelMultisetType( dataType );
		}
	}

	@Override
	public boolean isIntegerType() throws IOException
	{
		try (final IHDF5Reader reader = HDF5Factory.openForReading( groupProperty.get() ))
		{
			final Class< ? > dataType = reader.getDataSetInformation( dataset.get() ).getTypeInformation().tryGetJavaType();
			final boolean signed = reader.getDataSetInformation( dataset.get() ).getTypeInformation().isSigned();
			return isIntegerType( dataType, signed );
		}
	}

	@Override
	public void updateDatasetInfo( final String dataset, final DatasetInfo info )
	{
		try (final IHDF5Reader reader = HDF5Factory.openForReading( this.groupProperty.get() ))
		{

			final int nDim = reader.object().getDimensions( dataset ).length;

			if ( reader.object().hasAttribute( dataset, AXIS_ORDER_KEY ) )
			{
				final AxisOrder ao = AxisOrder.valueOf( reader.string().getAttr( dataset, AXIS_ORDER_KEY ) );
				datasetInfo.defaultAxisOrderProperty().set( ao );
				datasetInfo.selectedAxisOrderProperty().set( ao );
			}
			else
			{
				final Optional< AxisOrder > ao = AxisOrder.defaultOrder( nDim );
				if ( ao.isPresent() )
					this.datasetInfo.defaultAxisOrderProperty().set( ao.get() );
				if ( this.datasetInfo.selectedAxisOrderProperty().isNull().get() || this.datasetInfo.selectedAxisOrderProperty().get().numDimensions() != nDim )
					this.axisOrder().set( ao.get() );
			}

			final Class< ? > type = reader.getDataSetInformation( dataset ).getTypeInformation().tryGetJavaType();
			final boolean signed = reader.getDataSetInformation( dataset ).getTypeInformation().isSigned();

			final boolean hasResolution = reader.object().hasAttribute( dataset, RESOLUTION_KEY );
			final boolean hasOffset = reader.object().hasAttribute( dataset, OFFSET_KEY );
			final boolean hasMin = reader.object().hasAttribute( dataset, MIN_KEY );
			final boolean hasMax = reader.object().hasAttribute( dataset, MAX_KEY );

			this.datasetInfo.setResolution( hasResolution ? invert( reader.float64().getArrayAttr( dataset, RESOLUTION_KEY ) ) : DoubleStream.generate( () -> 1.0 ).limit( nDim ).toArray() );
			this.datasetInfo.setOffset( hasOffset ? invert( reader.float64().getArrayAttr( dataset, OFFSET_KEY ) ) : new double[ nDim ] );
			this.datasetInfo.minProperty().set( hasMin ? reader.float64().getAttr( dataset, MIN_KEY ) : minForType( type, signed ) );
			this.datasetInfo.maxProperty().set( hasMax ? reader.float64().getAttr( dataset, MAX_KEY ) : maxForType( type, signed ) );
		}

	}

	@Override
	public List< String > discoverDatasetAt( final String at )
	{
		try (IHDF5Reader reader = HDF5Factory.openForReading( new File( at ) ))
		{
			final ArrayList< String > datasets = new ArrayList<>();
			H5Utils.getAllDatasetPaths( reader, "/", datasets );
			return datasets;
		}
	}

	public static double[] invert( final double[] array )
	{
		final double[] ret = new double[ array.length ];
		for ( int i = 0, k = array.length - 1; i < array.length; ++i, --k )
			ret[ k ] = array[ i ];
		return ret;
	}
}
