package bdv.bigcat.viewer;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.JCommander.Builder;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.DoubleConverter;

/**
 * Parameters used to initialize the application via command line
 * 
 * @author vleite
 *
 */
public class CommandLineParameters
{
	@Parameter( names = { "--type", "-t" }, description = "Data type to load (hdf5 or n5)." )
	public String type = "N5";

	@Parameter( names = { "--rFile", "-r" }, description = "Input file path/folder (hdf5/n5) for raw data." )
	public String rawFilePath = "";

	@Parameter( names = { "--rDataset", "-rd" }, description = "Dataset path for raw data. Refers to a path inside hdf5 file or relative to n5 main folder." )
	public String rawDatasetPath = "";

	@Parameter( names = { "--rMinIntensity", "-rmin" }, description = "Minimum value for raw data intensity.", converter = DoubleConverter.class )
	public Double minIntensity = 0.0;

	@Parameter( names = { "--rMaxIntensity", "-rmax" }, description = "Maximum value for raw data intensity.", converter = DoubleConverter.class )
	public Double maxIntensity = 255.0;

	@Parameter( names = { "--rxResolution", "-rxrs" }, description = "Value of resolution for raw data on x direction.", converter = DoubleConverter.class )
	public Double xRawResolution;

	@Parameter( names = { "--ryResolution", "-ryrs" }, description = "Value of resolution for raw data on y direction.", converter = DoubleConverter.class )
	public Double yRawResolution;

	@Parameter( names = { "--rzResolution", "-rzrs" }, description = "Value of resolution for raw data on z direction.", converter = DoubleConverter.class )
	public Double zRawResolution;

	@Parameter( names = { "--rxOffset", "-rxoff" }, description = "Value of offset for raw data on x direction.", converter = DoubleConverter.class )
	public Double xRawOffset = 0.0;

	@Parameter( names = { "--ryOffset", "-ryoff" }, description = "Value of offset for raw data on y direction.", converter = DoubleConverter.class )
	public Double yRawOffset = 0.0;

	@Parameter( names = { "--rzOffset", "-rzoff" }, description = "Value of offset for raw data on z direction.", converter = DoubleConverter.class )
	public Double zRawOffset = 0.0;

	@Parameter( names = { "--lFile", "-l" }, description = "Input file path/folder (hdf5/n5) for label data."
			+ " It can be ommited if labels are on the same path as the raw data." )
	public String labelFilePath = "";

	@Parameter( names = { "--lDataset", "-ld" }, description = "Dataset path for label data. Refers to a path inside hdf5 file or relative to n5 main folder." )
	public String labelDatasetPath = "";

	@Parameter( names = { "--help", "-h" }, help = true, description = "Show this help." )
	public static boolean help;

	private final static Builder builder = JCommander.newBuilder();

	public static boolean getParameters( CommandLineParameters params, String[] args )
	{
		// get the parameters
		builder.addObject( params ).build().parse( args );

		if ( help )
		{
			// show help and quit
			builder.build().usage();
			System.exit( 0 );
		}

		return true;
	}

	public static boolean hasRawParameters( CommandLineParameters params )
	{
		if ( params.rawFilePath.isEmpty() || params.rawDatasetPath.isEmpty() ||
				params.xRawResolution.toString().isEmpty() ||
				params.yRawResolution.toString().isEmpty() ||
				params.zRawResolution.toString().isEmpty() ) { return false; }

		return true;
	}

}
