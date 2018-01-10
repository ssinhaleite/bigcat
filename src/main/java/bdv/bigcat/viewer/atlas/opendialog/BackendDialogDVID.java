package bdv.bigcat.viewer.atlas.opendialog;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.function.Consumer;

import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;

import bdv.bigcat.viewer.atlas.data.DataSource;
<<<<<<< 7fc4eaa0faa423505b41da73bf0313ef7410c57c
import bdv.bigcat.viewer.atlas.data.LabelDataSource;
=======
>>>>>>> Fix build. LabelDataSource is not working yet.
import bdv.util.volatiles.SharedQueue;
import javafx.beans.property.DoubleProperty;
import javafx.beans.property.ObjectProperty;
import javafx.beans.property.SimpleDoubleProperty;
import javafx.beans.property.SimpleObjectProperty;
import javafx.beans.value.ObservableValue;
import javafx.scene.Node;
import javafx.scene.control.TextField;
import javafx.scene.effect.Effect;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.Priority;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;

public class BackendDialogDVID implements BackendDialog, CombinesErrorMessages
{

	// base url to api
	private final SimpleObjectProperty< String > dvid = new SimpleObjectProperty<>();

	// commit
	private final SimpleObjectProperty< String > commit = new SimpleObjectProperty<>();

	// dataset
	private final SimpleObjectProperty< String > dataset = new SimpleObjectProperty<>();

	// combined error messages
	private final SimpleObjectProperty< String > errorMessage = new SimpleObjectProperty<>();

	// error message for invalid dvid url
	private final SimpleObjectProperty< String > dvidError = new SimpleObjectProperty<>();

	// error message for invalid dataset
	private final SimpleObjectProperty< String > datasetError = new SimpleObjectProperty<>();

	// error message for invalid commit
	private final SimpleObjectProperty< String > commitError = new SimpleObjectProperty<>();

	private final SimpleObjectProperty< Effect > dvidErrorEffect = new SimpleObjectProperty<>();

	private final SimpleObjectProperty< Effect > commitErrorEffect = new SimpleObjectProperty<>();

	private final SimpleObjectProperty< Effect > datasetErrorEffect = new SimpleObjectProperty<>();

	private final SimpleObjectProperty< String > repoError = new SimpleObjectProperty<>();

	private final SimpleDoubleProperty resX = new SimpleDoubleProperty( Double.NaN );

	private final SimpleDoubleProperty resY = new SimpleDoubleProperty( Double.NaN );

	private final SimpleDoubleProperty resZ = new SimpleDoubleProperty( Double.NaN );

	private final SimpleDoubleProperty offX = new SimpleDoubleProperty( Double.NaN );

	private final SimpleDoubleProperty offY = new SimpleDoubleProperty( Double.NaN );

	private final SimpleDoubleProperty offZ = new SimpleDoubleProperty( Double.NaN );

	public BackendDialogDVID()
	{
		dvid.addListener( ( obs, oldv, newv ) -> {
			if ( newv != null && !newv.isEmpty() )
			{
				this.dvidError.set( null );
				updateMetaInformation();
			}
			else
			{
				this.dvidError.set( "No valid dvid url." );
			}
		} );

		commit.addListener( ( obs, oldv, newv ) -> {
			if ( newv != null && !newv.isEmpty() )
			{
				this.commitError.set( null );
				updateMetaInformation();
			}
			else
			{
				this.commitError.set( "No valid commit" );
			}
		} );

		dataset.addListener( ( obs, oldv, newv ) -> {
			if ( newv != null && !newv.isEmpty() )
			{
				this.datasetError.set( null );
				updateMetaInformation();
			}
			else
			{
				this.datasetError.set( "No valid dataset" );
			}
		} );

		dvidError.addListener( ( obs, oldv, newv ) -> this.dvidErrorEffect.set( newv != null && newv.length() > 0 ? textFieldErrorEffect : textFieldNoErrorEffect ) );
		commitError.addListener( ( obs, oldv, newv ) -> this.commitErrorEffect.set( newv != null && newv.length() > 0 ? textFieldErrorEffect : textFieldNoErrorEffect ) );
		datasetError.addListener( ( obs, oldv, newv ) -> this.datasetErrorEffect.set( newv != null && newv.length() > 0 ? textFieldErrorEffect : textFieldNoErrorEffect ) );

		this.errorMessages().forEach( em -> em.addListener( ( obs, oldv, newv ) -> combineErrorMessages() ) );

		dvid.set( "" );
		commit.set( "" );
		dataset.set( "" );
	}

	@Override
	public Node getDialogNode()
	{
		final TextField dvidURLField = new TextField( dvid.get() );
		dvidURLField.setMinWidth( 0 );
		dvidURLField.setMaxWidth( Double.POSITIVE_INFINITY );
		dvidURLField.setPromptText( "dvid url" );
		dvidURLField.textProperty().bindBidirectional( dvid );

		final TextField commitField = new TextField( commit.get() );
		commitField.setMinWidth( 0 );
		commitField.setMaxWidth( Double.POSITIVE_INFINITY );
		commitField.setPromptText( "commit" );
		commitField.textProperty().bindBidirectional( commit );

		final TextField datasetField = new TextField( dataset.get() );
		datasetField.setMinWidth( 0 );
		datasetField.setMaxWidth( Double.POSITIVE_INFINITY );
		datasetField.setPromptText( "dataset" );
		datasetField.textProperty().bindBidirectional( dataset );

		final GridPane grid = new GridPane();
		grid.add( dvidURLField, 0, 0 );
		grid.add( commitField, 0, 1 );
		grid.add( datasetField, 0, 2 );
		GridPane.setHgrow( dvidURLField, Priority.ALWAYS );
		GridPane.setHgrow( commitField, Priority.ALWAYS );
		GridPane.setHgrow( datasetField, Priority.ALWAYS );

		setErrorEffect( dvidURLField, this.dvidErrorEffect );
		setErrorEffect( commitField, this.commitErrorEffect );
		setErrorEffect( datasetField, this.datasetErrorEffect );

		return grid;
	}

	private void setErrorEffect( TextField textField, SimpleObjectProperty< Effect > effect )
	{
		effect.addListener( ( obs, oldv, newv ) -> {
			if ( !textField.isFocused() )
				textField.setEffect( newv );
		} );

		textField.setEffect( effect.get() );

		textField.focusedProperty().addListener( ( obs, oldv, newv ) -> {
			if ( newv )
				textField.setEffect( BackendDialog.textFieldNoErrorEffect );
			else
				textField.setEffect( effect.get() );
		} );
	}

	private void updateMetaInformation()
	{
		if ( ( dvid.get() != null ) && ( commit.get() != null ) && ( dataset.get() != null ) )
		{
			if ( dvid.get().isEmpty() || commit.get().isEmpty() || dataset.get().isEmpty() )
				return;

			String infoUrl = dvid.get() + "/" + commit.get() + "/" + dataset.get() + "/info";
			DVIDResponse response = null;
			try
			{
				response = DVIDParser.fetch( infoUrl, DVIDResponse.class );
			}
			catch ( JsonSyntaxException | JsonIOException | IOException e )
			{

				this.repoError.set( "no data/repository found" );
				return;
			}

			this.repoError.set( "" );
			if ( response.Extended.VoxelSize.length == 3 )
			{
				resX.set( response.Extended.VoxelSize[ 0 ] );
				resY.set( response.Extended.VoxelSize[ 1 ] );
				resZ.set( response.Extended.VoxelSize[ 2 ] );
			}

			if ( response.Extended.MinPoint.length == 3 )
			{
				offX.set( response.Extended.MinPoint[ 0 ] );
				offY.set( response.Extended.MinPoint[ 1 ] );
				offZ.set( response.Extended.MinPoint[ 2 ] );
			}
		}
	}

	@Override
	public ObjectProperty< String > errorMessage()
	{
		return errorMessage;
	}

	@Override
	public < T extends RealType< T > & NativeType< T >, V extends RealType< V > > Optional< DataSource< T, V > > getRaw(
			final String name,
			final double[] resolution,
			final double[] offset,
			final SharedQueue sharedQueue,
			final int priority ) throws IOException
	{
		final String rawURL = this.dvid.get();
		final String rawCommit = this.commit.get();
		final String rawDataset = this.dataset.get();

		return Optional.of( DataSource.createDVIDRawSource( name, rawURL, rawCommit, rawDataset, resolution, offset, sharedQueue, priority ) );
	}

	@Override
	public Optional< LabelDataSource< ?, ? > > getLabels(
			final String name,
			final double[] resolution,
			final double[] offset,
			final SharedQueue sharedQueue,
			final int priority ) throws IOException
	{
		return Optional.empty();
	}

	@Override
	public Collection< ObservableValue< String > > errorMessages()
	{
		return Arrays.asList( this.dvidError, this.commitError, this.datasetError, this.repoError );
	}

	@Override
	public Consumer< Collection< String > > combiner()
	{
		return strings -> this.errorMessage.set( String.join( "\n", strings ) );
	}

	@Override
	public DoubleProperty resolutionX()
	{
		return this.resX;
	}

	@Override
	public DoubleProperty resolutionY()
	{
		return this.resY;
	}

	@Override
	public DoubleProperty resolutionZ()
	{
		return this.resZ;
	}

	@Override
	public DoubleProperty offsetX()
	{
		return this.offX;
	}

	@Override
	public DoubleProperty offsetY()
	{
		return this.offY;
	}

	@Override
	public DoubleProperty offsetZ()
	{
		return this.offZ;
	}

}
