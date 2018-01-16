package bdv.bigcat.viewer.atlas;

import bdv.util.volatiles.SharedQueue;
import javafx.application.Application;
import javafx.application.Platform;
import javafx.stage.Stage;

public class AtlasApp extends Application
{

	@Override
	public void start( final Stage primaryStage ) throws Exception
	{
		final SharedQueue sharedQueue = new SharedQueue( 1, 20 );
		final Atlas atlas = new Atlas( sharedQueue );
		atlas.start( primaryStage );

		Platform.setImplicitExit( true );
	}

	public static void main( final String[] args )
	{
		launch( args );
	}


//	dvid.set( "http://emdata.janelia.org/api/node" );
//	commit.set( "822524777d3048b8bd520043f90c1d28" );
// raw
//	dataset.set( "grayscale" );
// groundtruth
//	dataset.set( "groundtruth" );
}
