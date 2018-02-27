package bdv.bigcat.viewer.atlas;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.function.Function;

import bdv.bigcat.viewer.CommandLineParameters;
import bdv.bigcat.viewer.atlas.data.DataSource;
import bdv.bigcat.viewer.atlas.opendialog.BackendDialogHDF5;
import bdv.bigcat.viewer.atlas.opendialog.BackendDialogN5;
import bdv.bigcat.viewer.atlas.opendialog.SourceFromRAI;
import bdv.net.imglib2.util.Triple;
import bdv.util.volatiles.SharedQueue;
import bdv.viewer.Interpolation;
import javafx.application.Application;
import javafx.application.Platform;
import javafx.stage.Stage;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.Volatile;
import net.imglib2.interpolation.InterpolatorFactory;
import net.imglib2.interpolation.randomaccess.NLinearInterpolatorFactory;
import net.imglib2.interpolation.randomaccess.NearestNeighborInterpolatorFactory;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.type.NativeType;
import net.imglib2.type.Type;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.volatiles.AbstractVolatileRealType;

public class AtlasApp extends Application
{
	static CommandLineParameters params = new CommandLineParameters();

	@Override
	public void start( final Stage primaryStage ) throws Exception
	{
		final SharedQueue sharedQueue = new SharedQueue( 1, 20 );
		final Atlas atlas = new Atlas( sharedQueue );
		atlas.start( primaryStage );

		Platform.setImplicitExit( true );

		if ( CommandLineParameters.hasRawParameters( params ) )
		{
			loadRaw( params.rawDatasetPath.substring( params.rawDatasetPath.lastIndexOf( "/" ), params.rawDatasetPath.length() - 1 ),
					sharedQueue, sharedQueue.getNumPriorities() - 1,
					params.rawFilePath, params.rawDatasetPath,
					new double[] { params.xRawResolution, params.yRawResolution, params.zRawResolution },
					new double[] { params.xRawOffset, params.yRawOffset, params.zRawOffset },
					atlas );
		}

	}

	public static void main( final String[] args )
	{
		CommandLineParameters.getParameters( params, args );
		launch( args );
	}

	@SuppressWarnings( "unchecked" )
	private < T extends RealType< T > & NativeType< T >, V extends AbstractVolatileRealType< T, V > & NativeType< V > > void loadRaw(
			final String name,
			final SharedQueue sharedQueue,
			final int priority,
			String group,
			String dataset,
			double[] resolution,
			double[] offset,
			Atlas atlas ) throws IOException
	{
		final Triple< RandomAccessibleInterval< T >[], RandomAccessibleInterval< V >[], AffineTransform3D[] > dataAndVolatile = getTriple( name, sharedQueue, priority, group, dataset, resolution, offset, atlas );
		final Collection< ? extends DataSource< ? extends RealType< ? >, ? extends RealType< ? > > > raws = getCached( dataAndVolatile.getA(), dataAndVolatile.getB(), dataAndVolatile.getC(), interpolation -> interpolation.equals( Interpolation.NLINEAR ) ? new NLinearInterpolatorFactory<>() : new NearestNeighborInterpolatorFactory<>(),
				interpolation -> interpolation.equals( Interpolation.NLINEAR ) ? new NLinearInterpolatorFactory<>() : new NearestNeighborInterpolatorFactory<>(),
				name, sharedQueue, priority );
		atlas.addRawSources( ( Collection ) raws, params.minIntensity, params.maxIntensity );

	}

	private < T extends NativeType< T >, V extends Volatile< T > > Triple< RandomAccessibleInterval< T >[], RandomAccessibleInterval< V >[], AffineTransform3D[] > getTriple( final String name,
			final SharedQueue sharedQueue,
			final int priority,
			String group,
			String dataset,
			double[] resolution,
			double[] offset,
			Atlas atlas ) throws IOException
	{
		switch ( params.type.toUpperCase() )
		{
		case "HDF5":
			BackendDialogHDF5 hdf5 = new BackendDialogHDF5();
			return hdf5.getDataAndVolatile( sharedQueue, priority, group, dataset, resolution, offset );
		case "N5":
		default:
			BackendDialogN5 n5 = new BackendDialogN5();
			final Boolean isLabelMultisetType = n5.getAttribute( group, dataset, "isLabelMultiset", Boolean.class );
			return n5.getDataAndVolatile( sharedQueue, priority, Optional.ofNullable( isLabelMultisetType ).orElse( false ), group, dataset, resolution, offset );
		}
	}

	private static < T extends Type< T >, V extends Type< V > > Collection< DataSource< T, V > > getCached(
			final RandomAccessibleInterval< T >[] rai,
			final RandomAccessibleInterval< V >[] vrai,
			final AffineTransform3D[] transforms,
			final Function< Interpolation, InterpolatorFactory< T, RandomAccessible< T > > > interpolation,
			final Function< Interpolation, InterpolatorFactory< V, RandomAccessible< V > > > vinterpolation,
			final String nameOrPattern,
			final SharedQueue sharedQueue,
			final int priority ) throws IOException
	{
		return Arrays.asList( SourceFromRAI.getAsSource( rai, vrai, transforms, interpolation, vinterpolation, nameOrPattern ) );
	}

}
