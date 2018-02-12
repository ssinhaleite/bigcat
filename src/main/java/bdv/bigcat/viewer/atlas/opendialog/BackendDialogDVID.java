package bdv.bigcat.viewer.atlas.opendialog;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.janelia.saalfeldlab.n5.DataType;

import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;

import bdv.bigcat.viewer.atlas.opendialog.OpenSourceDialog.TYPE;
import bdv.bigcat.viewer.state.FragmentSegmentAssignmentOnlyLocal;
import bdv.bigcat.viewer.state.FragmentSegmentAssignmentState;
import bdv.net.imglib2.util.Triple;
import bdv.net.imglib2.util.ValueTriple;
import bdv.util.volatiles.SharedQueue;
import bdv.util.volatiles.VolatileViews;
import javafx.beans.binding.Bindings;
import javafx.beans.binding.StringBinding;
import javafx.beans.property.DoubleProperty;
import javafx.beans.property.ObjectProperty;
import javafx.beans.property.SimpleObjectProperty;
import javafx.beans.value.ObservableStringValue;
import javafx.beans.value.ObservableValue;
import javafx.scene.Node;
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

	// dataset
	private final SimpleObjectProperty< String > dataset = new SimpleObjectProperty<>();

	// combined error messages
	private final SimpleObjectProperty< String > errorMessage = new SimpleObjectProperty<>();

	// no url defined
	private final SimpleObjectProperty< String > dvidURLError = new SimpleObjectProperty<>();

	// no repository defined
	private final SimpleObjectProperty< String > repoUUIDError = new SimpleObjectProperty<>();

	// couldn't find the repo informed
	private final SimpleObjectProperty< String > invalidURLError = new SimpleObjectProperty<>();

	// the number of dimensions is less than 3 or bigger than 5
	private final SimpleObjectProperty< String > axisError = new SimpleObjectProperty<>();

	// error message for invalid dataset
	private final SimpleObjectProperty< String > datasetError = new SimpleObjectProperty<>();

	private final SimpleObjectProperty< Effect > dvidErrorEffect = new SimpleObjectProperty<>();

	private final SimpleObjectProperty< Effect > commitErrorEffect = new SimpleObjectProperty<>();

	private final SimpleObjectProperty< Effect > datasetErrorEffect = new SimpleObjectProperty<>();

	private final Effect textFieldErrorEffect = new InnerShadow( 10, Color.ORANGE );

	private final Effect textFieldNoErrorEffect = new TextField().getEffect();

	// TODO: should it overwrite the name added by the user?
	private final StringBinding name = Bindings.createStringBinding( () -> {
		return dataset.get();
	}, dataset );

	private final DatasetInfo datasetInfo = new DatasetInfo();

	private DVIDResponse response = null;

	private TYPE type = TYPE.RAW;

	public BackendDialogDVID()
	{
		dvidURL.addListener( ( obs, oldv, newv ) -> {
			if ( newv != null && !newv.isEmpty() )
			{
				this.dvidURLError.set( null );
				updateMetaInformation();
			}
			else
			{
				this.dvidURLError.set( "No valid dvid url." );
			}
		} );

		repoUUID.addListener( ( obs, oldv, newv ) -> {
			if ( newv != null && !newv.isEmpty() )
			{
				this.repoUUIDError.set( null );
				updateMetaInformation();
			}
			else
			{
				this.repoUUIDError.set( "No valid commit" );
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

		dvidURLError.addListener( ( obs, oldv, newv ) -> this.dvidErrorEffect.set( newv != null && newv.length() > 0 ? textFieldErrorEffect : textFieldNoErrorEffect ) );
		repoUUIDError.addListener( ( obs, oldv, newv ) -> this.commitErrorEffect.set( newv != null && newv.length() > 0 ? textFieldErrorEffect : textFieldNoErrorEffect ) );
		datasetError.addListener( ( obs, oldv, newv ) -> this.datasetErrorEffect.set( newv != null && newv.length() > 0 ? textFieldErrorEffect : textFieldNoErrorEffect ) );

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

		final TextField commitField = new TextField( repoUUID.get() );
		commitField.setMinWidth( 0 );
		commitField.setMaxWidth( Double.POSITIVE_INFINITY );
		commitField.setPromptText( "commit" );
		commitField.textProperty().bindBidirectional( repoUUID );

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
				textField.setEffect( textFieldNoErrorEffect );
			else
				textField.setEffect( effect.get() );
		} );
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
				response = DVIDParser.fetch( infoUrl, DVIDResponse.class );
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

	@Override
	public ObjectProperty< String > errorMessage()
	{
		return errorMessage;
	}

	@SuppressWarnings( "unchecked" )
	@Override
	public Collection< ObservableValue< String > > errorMessages()
	{
		return Arrays.asList( this.dvidURLError, this.repoUUIDError, this.datasetError, this.invalidURLError, this.axisError );
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

}
