#include <CGAL/Exact_predicates_exact_constructions_kernel.h>
#include <CGAL/Boolean_set_operations_2.h>
#include <list>

#include <CGAL/Cartesian.h>
#include <CGAL/centroid.h>
#include <CGAL/squared_distance_2.h>
#include <CGAL/bounding_box.h>
#include <CGAL/number_utils.h>

typedef CGAL::Exact_predicates_exact_constructions_kernel Kernel;
typedef Kernel::Point_2                                   Point_2;
typedef CGAL::Polygon_2<Kernel>                           Polygon_2;
typedef CGAL::Polygon_with_holes_2<Kernel>                Polygon_with_holes_2;
typedef std::list<Polygon_with_holes_2>                   Pwh_list_2;


#include <SpatialIndex.h>

using namespace SpatialIndex;
using namespace std;


class MyDataStream : public IDataStream
{
public:
	MyDataStream(std::string inputFile) : m_pNext(0)
	{
		id=0;
		m_fin.open(inputFile.c_str());

		if (! m_fin)
			throw Tools::IllegalArgumentException("Input file not found.");

		readNextEntry();
	}

	virtual ~MyDataStream()
	{
		if (m_pNext != 0) delete m_pNext;
	}

	virtual IData* getNext()
	{
		if (m_pNext == 0) return 0;

		RTree::Data* ret = m_pNext;
		m_pNext = 0;
		readNextEntry();
		return ret;
	}

	virtual bool hasNext()
	{
		return (m_pNext != 0);
	}

	virtual uint32_t size()
	{
		throw Tools::NotSupportedException("Operation not supported.");
	}

	virtual void rewind()
	{
		if (m_pNext != 0)
		{
			delete m_pNext;
			m_pNext = 0;
		}

		m_fin.seekg(0, std::ios::beg);
		readNextEntry();
	}

	void readNextEntry()
	{
		uint32_t op;
		double low[2], high[2];
		std::string line;

		if (m_fin.good())
		{
			getline(m_fin,line);
			if (line.length()==0) return;
			std::string token;
		   	std::istringstream iss(line);
			Polygon_2 P;
			for (int i=-2; getline(iss, token, ','); i++) { 
				if (i<0) continue;
				if (i==0) token.erase(0,1);
				else if (token[token.length()-1]=='"') token.erase(token.length()-1,1);
				std::istringstream iss2(token);
				int x,y;
				iss2>>x>>y;
  				P.push_back (Point_2 (x, y));
				//std::cout << x<<' '<<y << std::endl;
			}
  			//std::cout<<"P: "<<P<<std::endl;
			
			//Kernel::Iso_rectangle_2 mbr = CGAL::bounding_box(P.vertices_begin(), P.vertices_end());
  			//std::cout<<"minimal bounding rectangle: "<<mbr<<std::endl;
			//low[0] = CGAL::to_double(mbr.vertex(0).x());
			//low[1] = CGAL::to_double(mbr.vertex(0).y());
			//high[0] = CGAL::to_double(mbr.vertex(1).x());
			//high[1] = CGAL::to_double(mbr.vertex(1).y());

			CGAL::Bbox_2 mbr = P.bbox();
			low[0] = CGAL::to_double(mbr.xmin());
			low[1] = CGAL::to_double(mbr.ymin());
			high[0] = CGAL::to_double(mbr.xmax());
			high[1] = CGAL::to_double(mbr.ymax());

			Region r(low, high, 2);

			std::ostringstream os;
			// the polygon is stored using delta x and delta y in the rtree leaf node.
			os<<P.size();
			int index=0;
			double lastx, lasty;	
			CGAL::Polygon_2<Kernel>::Vertex_iterator cv;
			for (cv=P.vertices_begin();cv!=P.vertices_end();++cv) {
				if (index==0) os<<' '<<cv->x()<<' '<<cv->y();
				else os<<' '<<cv->x()-lastx<<' '<<cv->y()-lasty;
				lastx=CGAL::to_double(cv->x());
				lasty=CGAL::to_double(cv->y());
				index++;
			}
			//os << P;
			std::string data = os.str();
			m_pNext = new RTree::Data(data.size()+1, reinterpret_cast<const byte*>(data.c_str()), r, id++);
				// Associate a bogus data array with every entry for testing purposes.
				// Once the data array is given to RTRee:Data a local copy will be created.
				// Hence, the input data array can be deleted after this operation if not
				// needed anymore.
		}
	}

	std::ifstream m_fin;
	RTree::Data* m_pNext;
	id_type id;
};

int main(int argc, char** argv)
{
	try
	{
		if (argc != 5)
		{
			std::cerr << "Usage: " << argv[0] << " input_file tree_file capacity(default=100) utilization(default=0.7)." << std::endl;
			return -1;
		}

		std::string baseName = argv[2];
		double utilization = atof(argv[4]);

		IStorageManager* diskfile = StorageManager::createNewDiskStorageManager(baseName, 4096);
			// Create a new storage manager with the provided base name and a 4K page size.

		StorageManager::IBuffer* file = StorageManager::createNewRandomEvictionsBuffer(*diskfile, 10, false);
			// applies a main memory random buffer on top of the persistent storage manager
			// (LRU buffer, etc can be created the same way).

		MyDataStream stream(argv[1]);

		// Create and bulk load a new RTree with dimensionality 2, using "file" as
		// the StorageManager and the RSTAR splitting policy.
		id_type indexIdentifier;
		ISpatialIndex* tree = RTree::createAndBulkLoadNewRTree(
			RTree::BLM_STR, stream, *file, utilization, atoi(argv[3]), atoi(argv[3]), 2, SpatialIndex::RTree::RV_RSTAR, indexIdentifier);

		std::cerr << *tree;
		std::cerr << "Buffer hits: " << file->getHits() << std::endl;
		std::cerr << "Index ID: " << indexIdentifier << std::endl;

		bool ret = tree->isIndexValid();
		if (ret == false) std::cerr << "ERROR: Structure is invalid!" << std::endl;
		else std::cerr << "The stucture seems O.K." << std::endl;

		delete tree;
		delete file;
		delete diskfile;
			// delete the buffer first, then the storage manager
			// (otherwise the the buffer will fail trying to write the dirty entries).
	}
	catch (Tools::Exception& e)
	{
		std::cerr << "******ERROR******" << std::endl;
		std::string s = e.what();
		std::cerr << s << std::endl;
		return -1;
	}

	return 0;
}
