package bdv.bigcat.annotation;

import bdv.util.IdService;
import ch.systemsx.cisd.base.mdarray.MDFloatArray;
import ch.systemsx.cisd.base.mdarray.MDLongArray;
import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.IHDF5Reader;
import ch.systemsx.cisd.hdf5.IHDF5Writer;
import ncsa.hdf.hdf5lib.exceptions.HDF5SymbolTableException;
import net.imglib2.RealPoint;

public class AnnotationsHdf5Store implements AnnotationsStore {
	
	private String filename;
	private String groupname;
	
	public AnnotationsHdf5Store(String filename, String groupname) {
		
		this.filename = filename;
		this.groupname = groupname;
	}

	@Override
	public Annotations read() {
		
		Annotations annotations = new Annotations();
	
		final IHDF5Reader reader = HDF5Factory.openForReading(filename);
		readSynapses(annotations, reader);
		readPreSynapticSites(annotations, reader);
		readPostSynapticSites(annotations, reader);
		
		return annotations;
	}
	
	interface AnnotationFactory {
		Annotation create(long id, RealPoint pos, String comment);
	}

	private void readAnnotations(Annotations annotations, final IHDF5Reader reader, String name, AnnotationFactory factory) {
		
		final MDFloatArray locations;
		final MDLongArray ids;

		try {
			locations = reader.float32().readMDArray(groupname + "/" + name + "_locations");
			ids = reader.uint64().readMDArray(groupname + "/" + name + "_ids");
		} catch (HDF5SymbolTableException e) {
			System.out.println("HDF5 file does not contain annotations for " + name);
			return;
		}
			
		final int[] dims = locations.dimensions();
		
		for (int i = 0; i < dims[0]; i++) {
			
			float p[] = {
				locations.get(i, 0),
				locations.get(i, 1),
				locations.get(i, 2),
			};
			RealPoint pos = new RealPoint(3);
			pos.setPosition(p);
			
			long id = ids.get(i);
			IdService.invalidate(id);
			Annotation annotation = factory.create(id, pos, "");
			annotations.add(annotation);
		}
	}

	private void readSynapses(Annotations annotations, final IHDF5Reader reader) {
		
		class Factory implements AnnotationFactory {
			public Annotation create(long id, RealPoint pos, String comment) {
				return new Synapse(id, pos, comment);
			}
		}
		
		readAnnotations(annotations, reader, "synapse", new Factory());
	}

	private void readPreSynapticSites(Annotations annotations, final IHDF5Reader reader) {
		
		class Factory implements AnnotationFactory {
			public Annotation create(long id, RealPoint pos, String comment) {
				return new PreSynapticSite(id, pos, comment);
			}
		}
		
		readAnnotations(annotations, reader, "presynaptic_site", new Factory());
	}

	private void readPostSynapticSites(Annotations annotations, final IHDF5Reader reader) {
		
		class Factory implements AnnotationFactory {
			public Annotation create(long id, RealPoint pos, String comment) {
				return new PostSynapticSite(id, pos, comment);
			}
		}
		
		readAnnotations(annotations, reader, "postsynaptic_site", new Factory());
		
		final MDLongArray prePostPartners;
		
		try {
			prePostPartners = reader.uint64().readMDArray(groupname + "/pre_post_partners");
		} catch (HDF5SymbolTableException e) {
			return;
		}
		
		for (int i = 0; i < prePostPartners.dimensions()[0]; i++) {
			long pre = prePostPartners.get(i, 0);
			long post = prePostPartners.get(i, 1);
			PreSynapticSite preSite = (PreSynapticSite)annotations.getById(pre);
			PostSynapticSite postSite = (PostSynapticSite)annotations.getById(post);
			
			preSite.setPartner(postSite);
			postSite.setPartner(preSite);
		}
	}

	@Override
	public void write(Annotations annotations) {

		final IHDF5Writer writer = HDF5Factory.open(filename);
		
		class AnnotationsCounter extends AnnotationVisitor {
			
			public int numSynapses = 0;
			public int numPreSynapticSites = 0;
			public int numPostSynapticSites = 0;
			
			@Override
			public void visit(Synapse synapse) {
				numSynapses++;
			}

			@Override
			public void visit(PreSynapticSite preSynapticSite) {
				numPreSynapticSites++;
			}

			@Override
			public void visit(PostSynapticSite postSynapticSite) {
				numPostSynapticSites++;
			}
		}

		final AnnotationsCounter counter = new AnnotationsCounter();
		for (Annotation a : annotations.getAnnotations())
			a.accept(counter);
		
		class AnnotationsCrawler extends AnnotationVisitor {
			
			float[][] synapseData = new float[counter.numSynapses][3];
			long[] synapseIds = new long[counter.numSynapses];
			float[][] preSynapticSiteData = new float[counter.numPreSynapticSites][3];
			long[] preSynapticSiteIds = new long[counter.numPreSynapticSites];
			float[][] postSynapticSiteData = new float[counter.numPostSynapticSites][3];
			long[] postSynapticSiteIds = new long[counter.numPostSynapticSites];
			long[][] partners  = new long[counter.numPostSynapticSites][2];
			
			private int synapseIndex = 0;
			private int preIndex = 0;
			private int postIndex = 0;
			
			private void fillPosition(float[] data, Annotation a) {
				
				for (int i = 0; i < 3; i++)
					data[i] = a.getPosition().getFloatPosition(i);
			}
			
			@Override
			public void visit(Synapse synapse) {
				fillPosition(synapseData[synapseIndex], synapse);
				synapseIds[synapseIndex] = synapse.getId();
				synapseIndex++;
			}

			@Override
			public void visit(PreSynapticSite preSynapticSite) {
				fillPosition(preSynapticSiteData[preIndex], preSynapticSite);
				preSynapticSiteIds[preIndex] = preSynapticSite.getId();
				preIndex++;
			}

			@Override
			public void visit(PostSynapticSite postSynapticSite) {
				fillPosition(postSynapticSiteData[postIndex], postSynapticSite);
				postSynapticSiteIds[postIndex] = postSynapticSite.getId();
				partners[postIndex][0] = postSynapticSite.getPartner().getId();
				partners[postIndex][1] = postSynapticSite.getId();
				postIndex++;
			}
		}
		
		AnnotationsCrawler crawler = new AnnotationsCrawler();
		for (Annotation a : annotations.getAnnotations())
			a.accept(crawler);
		
		try {
			writer.createGroup(groupname);
		} catch (HDF5SymbolTableException e) {
			// nada
		}
		writer.float32().writeMatrix(groupname + "/synapse_locations", crawler.synapseData);
		writer.uint64().writeArray(groupname + "/synapse_ids", crawler.synapseIds);
		writer.float32().writeMatrix(groupname + "/presynaptic_site_locations", crawler.preSynapticSiteData);
		writer.uint64().writeArray(groupname + "/presynaptic_site_ids", crawler.preSynapticSiteIds);
		writer.float32().writeMatrix(groupname + "/postsynaptic_site_locations", crawler.postSynapticSiteData);
		writer.uint64().writeArray(groupname + "/postsynaptic_site_ids", crawler.postSynapticSiteIds);
		writer.uint64().writeMatrix(groupname + "/pre_post_partners", crawler.partners);
	}
}