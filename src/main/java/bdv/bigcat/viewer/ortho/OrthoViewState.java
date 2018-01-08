package bdv.bigcat.viewer.ortho;

import java.util.Collection;
import java.util.stream.Collectors;

import bdv.bigcat.viewer.state.GlobalTransformManager;
import bdv.viewer.Interpolation;
import bdv.viewer.Source;
import bdv.viewer.SourceAndConverter;
import bdv.viewer.ViewerOptions;
import javafx.beans.property.IntegerProperty;
import javafx.beans.property.ObjectProperty;
import javafx.beans.property.SimpleIntegerProperty;
import javafx.beans.property.SimpleObjectProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;

public class OrthoViewState
{

	protected final GridConstraintsManager constraintsManager;

	protected final GlobalTransformManager globalTransform;

	protected final ViewerOptions viewerOptions;

	protected final ObservableList< SourceAndConverter< ? > > sacs = FXCollections.observableArrayList();

	protected final SimpleObjectProperty< Interpolation > interpolation = new SimpleObjectProperty<>( Interpolation.NEARESTNEIGHBOR );

	protected final SimpleObjectProperty< Source< ? > > currentSource = new SimpleObjectProperty<>( null );

	private final IntegerProperty time = new SimpleIntegerProperty();

	public OrthoViewState()
	{
		this( ViewerOptions.options() );
	}

	public OrthoViewState( final ViewerOptions viewerOptions )
	{
		this( viewerOptions, new GlobalTransformManager(), new GridConstraintsManager() );
	}

	public OrthoViewState(
			final ViewerOptions viewerOptions,
			final GlobalTransformManager globalTransform,
			final GridConstraintsManager constraintsManager )
	{
		this.viewerOptions = viewerOptions;
		this.globalTransform = globalTransform;
		this.constraintsManager = constraintsManager;
	}

	public synchronized void addSource( final SourceAndConverter< ? > source )
	{
		this.sacs.add( source );
	}

	public synchronized void addSources( final Collection< ? extends SourceAndConverter< ? > > sources )
	{
		this.sacs.addAll( sources );
	}

	public synchronized void removeSource( final Source< ? > source )
	{
		sacs.removeAll( sacs.stream().filter( spimSource -> spimSource.getSpimSource().equals( source ) ).collect( Collectors.toList() ) );
	}

	public synchronized void removeAllSources()
	{
		sacs.clear();
	}

	public synchronized void setSources( final Collection< ? extends SourceAndConverter< ? > > sources )
	{
		sacs.setAll( sources );
	}

	public ObjectProperty< Source< ? > > currentSourceProperty()
	{
		return this.currentSource;
	}

	public GlobalTransformManager transformManager()
	{
		return this.globalTransform;
	}

	public IntegerProperty timeProperty()
	{
		return this.time;
	}

}
