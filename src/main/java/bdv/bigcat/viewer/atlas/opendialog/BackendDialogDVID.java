package bdv.bigcat.viewer.atlas.opendialog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.janelia.saalfeldlab.n5.DataType;

import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.googlecode.gentyref.TypeToken;

import bdv.bigcat.viewer.atlas.opendialog.OpenSourceDialog.TYPE;
import bdv.bigcat.viewer.atlas.opendialog.dvid.DVIDParser;
import bdv.bigcat.viewer.atlas.opendialog.dvid.DVIDUtils;
import bdv.bigcat.viewer.atlas.opendialog.dvid.DatasetInstanceDVID;
import bdv.bigcat.viewer.atlas.opendialog.dvid.RepoInstanceDVID;
import bdv.bigcat.viewer.state.FragmentSegmentAssignmentOnlyLocal;
import bdv.bigcat.viewer.state.FragmentSegmentAssignmentState;
import bdv.bigcat.viewer.util.InvokeOnJavaFXApplicationThread;
import bdv.net.imglib2.util.Triple;
import bdv.net.imglib2.util.ValueTriple;
import bdv.util.volatiles.SharedQueue;
import bdv.util.volatiles.VolatileViews;
import javafx.beans.binding.Bindings;
import javafx.beans.binding.StringBinding;
import javafx.beans.property.DoubleProperty;
import javafx.beans.property.ObjectProperty;
import javafx.beans.property.SimpleBooleanProperty;
import javafx.beans.property.SimpleObjectProperty;
import javafx.beans.value.ObservableStringValue;
import javafx.beans.value.ObservableValue;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.scene.Node;
import javafx.scene.control.ComboBox;
import javafx.scene.control.TextField;
import javafx.scene.effect.Effect;
import javafx.scene.effect.InnerShadow;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.Priority;
import javafx.scene.paint.Color;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.Volatile;
import net.imglib2.cache.volatiles.CacheHints;
import net.imglib2.cache.volatiles.LoadingStrategy;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.type.NativeType;

public class BackendDialogDVID implements SourceFromRAI, CombinesErrorMessages
{
	private final SimpleObjectProperty< String > dvidURL = new SimpleObjectProperty<>();

	private final SimpleObjectProperty< String > repoUUID = new SimpleObjectProperty<>();

	private final ObservableList< String > repoUUIDChoices = FXCollections.observableArrayList();

	private final SimpleObjectProperty< String > dataset = new SimpleObjectProperty<>();

	private final ObservableList< String > datasetChoices = FXCollections.observableArrayList();

	// combined error messages
	private final SimpleObjectProperty< String > errorMessage = new SimpleObjectProperty<>();

	// no url defined
	private final SimpleObjectProperty< String > dvidURLError = new SimpleObjectProperty<>();

	// no repository selected
	private final SimpleObjectProperty< String > repoUUIDError = new SimpleObjectProperty<>();

	// no dataset selected
	private final SimpleObjectProperty< String > datasetError = new SimpleObjectProperty<>();

	// couldn't find repo/dataset on informed url
	private final SimpleObjectProperty< String > invalidURLError = new SimpleObjectProperty<>();

	private final SimpleObjectProperty< Effect > dvidErrorEffect = new SimpleObjectProperty<>();

	private final Effect textFieldErrorEffect = new InnerShadow( 10, Color.ORANGE );

	private final Effect textFieldNoErrorEffect = new TextField().getEffect();

	private final SimpleBooleanProperty isRepoUUIDDropDownReady = new SimpleBooleanProperty();

	private final SimpleBooleanProperty isDatasetDropDownReady = new SimpleBooleanProperty();

	// TODO: should it overwrite the name added by the user?
	private final StringBinding name = Bindings.createStringBinding( () -> {
		return dataset.get();
	}, dataset );

	private final DatasetInfo datasetInfo = new DatasetInfo();

	private DatasetInstanceDVID response = null;

	private Map< String, RepoInstanceDVID > repoInfo = null;

	private TYPE type = TYPE.RAW;

	public BackendDialogDVID()
	{
		dvidURL.addListener( ( obs, oldv, newv ) -> {
			if ( newv != null && !newv.isEmpty() && !newv.equals( oldv ) )
			{
				this.dvidURLError.set( null );
				ArrayList< String > repositories = updateRepositories();
				if ( repositories != null && !repositories.isEmpty() )
				{
					InvokeOnJavaFXApplicationThread.invoke( () -> repoUUIDChoices.setAll( repositories ) );
					InvokeOnJavaFXApplicationThread.invoke( () -> this.repoUUID.set( null ) );
					this.isRepoUUIDDropDownReady.set( true );
					this.invalidURLError.set( null );
				}
			}
			else
			{
				isRepoUUIDDropDownReady.set( false );
				this.dvidURLError.set( "Not a valid dvid url" );
			}
		} );

		repoUUID.addListener( ( obs, oldv, newv ) -> {
			if ( newv != null && !newv.isEmpty() && !newv.equals( oldv ) )
			{
				this.repoUUIDError.set( null );
				ArrayList< String > datasets = updateDatasets();
				if ( datasets != null & !datasets.isEmpty() )
				{
					InvokeOnJavaFXApplicationThread.invoke( () -> datasetChoices.setAll( datasets ) );
					InvokeOnJavaFXApplicationThread.invoke( () -> this.dataset.set( null ) );
					this.isDatasetDropDownReady.set( true );
					this.invalidURLError.set( null );
				}
				else
					this.invalidURLError.set( "No dataset was found on the server" );
			}
			else
			{
				isDatasetDropDownReady.set( false );
				this.repoUUIDError.set( "No repository selected" );
			}
		} );

		dataset.addListener( ( obs, oldv, newv ) -> {
			if ( newv != null && !newv.isEmpty() && !newv.equals( oldv ) )
			{
				this.datasetError.set( null );
				updateMetaInformation();
			}
			else
			{
				this.datasetError.set( "No dataset selected" );
			}
		} );

		dvidURLError.addListener( ( obs, oldv, newv ) -> this.dvidErrorEffect.set( newv != null && newv.length() > 0 ? textFieldErrorEffect : textFieldNoErrorEffect ) );
		this.errorMessages().forEach( em -> em.addListener( ( obs, oldv, newv ) -> combineErrorMessages() ) );

		dvidURL.set( "" );
		repoUUID.set( "" );
		dataset.set( "" );
	}

	@Override
	public Node getDialogNode()
	{
		final TextField dvidURLField = new TextField( dvidURL.get() );
		dvidURLField.setMinWidth( 0 );
		dvidURLField.setMaxWidth( Double.POSITIVE_INFINITY );
		dvidURLField.setPromptText( "dvid url" );
		dvidURLField.textProperty().bindBidirectional( dvidURL );

		final ComboBox< String > repoUUIDDropDown = new ComboBox<>( repoUUIDChoices );
		repoUUIDDropDown.setPromptText( "repo UUID" );
		repoUUIDDropDown.setEditable( false );
		repoUUIDDropDown.valueProperty().bindBidirectional( repoUUID );
		repoUUIDDropDown.setMinWidth( 0 );
		repoUUIDDropDown.setMaxWidth( Double.POSITIVE_INFINITY );
		repoUUIDDropDown.disableProperty().bind( isRepoUUIDDropDownReady.not() );

		final ComboBox< String > datasetDropDown = new ComboBox<>( datasetChoices );
		datasetDropDown.setPromptText( "dataset" );
		datasetDropDown.setEditable( false );
		datasetDropDown.valueProperty().bindBidirectional( dataset );
		datasetDropDown.setMinWidth( 0 );
		datasetDropDown.setMaxWidth( Double.POSITIVE_INFINITY );
		datasetDropDown.disableProperty().bind( isDatasetDropDownReady.not() );

		final GridPane grid = new GridPane();
		grid.add( dvidURLField, 0, 0 );
		grid.add( repoUUIDDropDown, 0, 1 );
		grid.add( datasetDropDown, 0, 2 );
		GridPane.setHgrow( dvidURLField, Priority.ALWAYS );
		GridPane.setHgrow( repoUUIDDropDown, Priority.ALWAYS );
		GridPane.setHgrow( datasetDropDown, Priority.ALWAYS );

		setErrorEffect( dvidURLField, this.dvidErrorEffect );

		return grid;
	}

	@Override
	public ObjectProperty< String > errorMessage()
	{
		return errorMessage;
	}

	@Override
	public Collection< ObservableValue< String > > errorMessages()
	{
		return Arrays.asList( this.dvidURLError, this.invalidURLError, this.repoUUIDError, this.datasetError );
	}

	@Override
	public Consumer< Collection< String > > combiner()
	{
		return strings -> this.errorMessage.set( String.join( "\n", strings ) );
	}

	@SuppressWarnings( "unchecked" )
	@Override
	public < T extends NativeType< T >, V extends Volatile< T > > Triple< RandomAccessibleInterval< T >[], RandomAccessibleInterval< V >[], AffineTransform3D[] > getDataAndVolatile( SharedQueue sharedQueue, int priority ) throws IOException
	{
		final String url = this.dvidURL.get();
		final String repoUUID = this.repoUUID.get();
		final String dataset = this.dataset.get();
		final double[] resolution = Arrays.stream( resolution() ).mapToDouble( DoubleProperty::get ).toArray();
		final double[] offset = Arrays.stream( offset() ).mapToDouble( DoubleProperty::get ).toArray();
		final AffineTransform3D transform = new AffineTransform3D();
		transform.set(
				resolution[ 0 ], 0, 0, offset[ 0 ],
				0, resolution[ 1 ], 0, offset[ 1 ],
				0, 0, resolution[ 2 ], offset[ 2 ] );
		LOG.debug( "Resolution={}", Arrays.toString( resolution ) );

		boolean isRaw = true;
		if ( type == TYPE.LABEL )
			isRaw = false;

		final RandomAccessibleInterval< T > rai = DVIDUtils.openVolatile( url, repoUUID, dataset, offset, isRaw );
		final RandomAccessibleInterval< V > vrai = VolatileViews.wrapAsVolatile( rai, sharedQueue, new CacheHints( LoadingStrategy.VOLATILE, priority, true ) );

		return new ValueTriple<>( new RandomAccessibleInterval[] { rai }, new RandomAccessibleInterval[] { vrai }, new AffineTransform3D[] { transform } );
	}

	@Override
	public boolean isLabelType() throws IOException
	{
		return isLabelType( getDataType() );
	}

	@Override
	public boolean isLabelMultisetType() throws IOException
	{
		return isLabelMultisetType( getDataType() );
	}

	@Override
	public boolean isIntegerType() throws IOException
	{
		return isIntegerType( getDataType() );
	}

	@Override
	public DoubleProperty min()
	{
		return this.datasetInfo.minProperty();
	}

	@Override
	public DoubleProperty max()
	{
		return this.datasetInfo.maxProperty();
	}

	@Override
	public void typeChanged( final TYPE type )
	{
		this.type = type;
	}

	@Override
	public DoubleProperty[] resolution()
	{
		return this.datasetInfo.spatialResolutionProperties();
	}

	@Override
	public DoubleProperty[] offset()
	{
		return this.datasetInfo.spatialOffsetProperties();
	}

	@Override
	public ObservableStringValue nameProperty()
	{
		return this.name;
	}

	@Override
	public String identifier()
	{
		return "DVID";
	}

	@Override
	public Iterator< ? extends FragmentSegmentAssignmentState< ? > > assignments()
	{
		return Stream.generate( FragmentSegmentAssignmentOnlyLocal::new ).iterator();
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
				textField.setEffect( textFieldNoErrorEffect );
			else
				textField.setEffect( effect.get() );
		} );
	}

	private ArrayList< String > updateRepositories()
	{
		String repoInfoURL = dvidURL.get() + "/api/repos/info";
		try
		{
			repoInfo = DVIDParser.fetch( repoInfoURL, new TypeToken< Map< String, RepoInstanceDVID > >()
			{}.getType() );

			final ArrayList< String > repositories = new ArrayList<>( repoInfo.keySet() );
			return repositories;

		}
		catch ( JsonSyntaxException | JsonIOException | IOException | IllegalArgumentException e )
		{
			this.invalidURLError.set( "No repository was found on the server" );
			return null;
		}
	}

	private ArrayList< String > updateDatasets()
	{
		try
		{
			final Map< String, DatasetInstanceDVID > dataInstances = repoInfo.get( repoUUID.get() ).DataInstances;

			final ArrayList< String > datasets = new ArrayList<>( dataInstances.keySet() );
			return datasets;

		}
		catch ( JsonSyntaxException | JsonIOException e )
		{
			this.invalidURLError.set( "No dataset was found on the server" );
			return null;
		}
	}

	private void updateMetaInformation()
	{
		if ( ( dvidURL.get() != null ) && ( repoUUID.get() != null ) && ( dataset.get() != null ) )
		{
			if ( dvidURL.get().isEmpty() || repoUUID.get().isEmpty() || dataset.get().isEmpty() )
				return;

			String infoUrl = dvidURL.get() + "/api/node/" + repoUUID.get() + "/" + dataset.get() + "/info";
			try
			{
				response = DVIDParser.fetch( infoUrl, DatasetInstanceDVID.class );
			}
			catch ( JsonSyntaxException | JsonIOException | IOException e )
			{

				this.invalidURLError.set( "no data/repository found" );
				return;
			}
			this.invalidURLError.set( "" );

			if ( response.Extended.VoxelSize.length == 3 )
				setResolution( response.Extended.VoxelSize );

			if ( response.Extended.MinPoint.length == 3 )
				setOffset( response.Extended.MinPoint );

			String type = "";
			if ( response.Extended.Values.size() > 0 )
				type = response.Extended.Values.get( 0 ).DataType;

			DataType datatype = DataType.fromString( type );
			this.datasetInfo.minProperty().set( minForType( datatype ) );
			this.datasetInfo.maxProperty().set( maxForType( datatype ) );
		}
	}

	private static double minForType( final DataType t )
	{
		return 0.0;
	}

	private static double maxForType( final DataType t )
	{
		switch ( t )
		{
		case UINT8:
			return 0xff;
		case UINT16:
			return 0xffff;
		case UINT32:
			return 0xffffffffl;
		case UINT64:
			return 2.0 * Long.MAX_VALUE;
		case INT8:
			return Byte.MAX_VALUE;
		case INT16:
			return Short.MAX_VALUE;
		case INT32:
			return Integer.MAX_VALUE;
		case INT64:
			return Long.MAX_VALUE;
		case FLOAT32:
		case FLOAT64:
			return 1.0;
		default:
			return 1.0;
		}
	}

	private DataType getDataType()
	{
		DataType datatype = null;

		if ( response != null )
			if ( response.Extended.Values.size() > 0 )
			{
				String type = response.Extended.Values.get( 0 ).DataType;
				datatype = DataType.fromString( type );
			}

		return datatype;
	}

	private static boolean isLabelType( final DataType type )
	{
		return isLabelMultisetType( type ) || isIntegerType( type );
	}

	private static boolean isLabelMultisetType( final DataType type )
	{
		return false;
	}

	private static boolean isIntegerType( final DataType type )
	{
		switch ( type )
		{
		case INT8:
		case INT16:
		case INT32:
		case INT64:
		case UINT8:
		case UINT16:
		case UINT32:
		case UINT64:
			return true;
		default:
			return false;
		}
	}
}
