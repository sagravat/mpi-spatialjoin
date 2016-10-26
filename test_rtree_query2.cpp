#include <math.h>
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

const int DEBUG=0;

template <class Kernel, class Container>
typename CGAL::Polygon_2<Kernel, Container>::FT
area( const CGAL::Polygon_with_holes_2<Kernel, Container>& pwh)
{
        typedef typename CGAL::Polygon_2<Kernel, Container>::FT                                         FT;
        FT result = pwh.outer_boundary().area();
        for (Polygon_with_holes_2::Hole_const_iterator jt = pwh.holes_begin();
                jt != pwh.holes_end(); ++jt)
        {
                result += jt->area();
        }
        return result;
}

template <class Kernel, class Container>
typename CGAL::Polygon_2<Kernel, Container>::FT
overlap_area(const CGAL::Polygon_2<Kernel, Container> &P,
                         const CGAL::Polygon_2<Kernel, Container> &Q)
{
        CGAL_precondition(P.is_simple());
        CGAL_precondition(Q.is_simple());

        typedef typename CGAL::Polygon_2<Kernel, Container>::FT                                         FT;
        Pwh_list_2 overlap;
        CGAL::intersection(P, Q, std::back_inserter(overlap));
        if (overlap.empty())
                return 0;
        // summing the areas and reducing the area of holes.
        FT result = 0;
        for (Pwh_list_2::iterator it = overlap.begin(); it != overlap.end(); ++it)
        {
                result += area(*it);
        }
        return result;
}


template <class Kernel, class Container>
typename CGAL::Polygon_2<Kernel, Container>::FT
union_area(const CGAL::Polygon_2<Kernel, Container> &P,
                         const CGAL::Polygon_2<Kernel, Container> &Q)
{
        CGAL_precondition(P.is_simple());
        CGAL_precondition(Q.is_simple());

        typedef typename CGAL::Polygon_2<Kernel, Container>::FT                                         FT;

        // Compute the union of P and Q.
        Polygon_with_holes_2 unionR;
        if (CGAL::join (P, Q, unionR))
                return area(unionR);
        else
                return P.area()+Q.area();
}


std::string getDataString(const IData* d) {
	byte* pData = 0;
	uint32_t cLen = 0;
	d->getData(cLen, &pData);
	string s = reinterpret_cast<char*>(pData);
	delete[] pData;
	return s;
}

void parsePolygon(std::string str,Polygon_2 &P) {
	std::istringstream iss2(str);
	int n;
	double x,y,lastx,lasty;
	iss2>>n;
	for (int i=0; i<n-1; i++) {
		iss2>>x>>y;
		if (i>0) {
			x+=lastx;
			y+=lasty;
		}
		lastx=x;
		lasty=y;
  		P.push_back (Point_2 (x, y));
	}
  	//std::cout<<"Polygon: "<<P<<std::endl;
}

// example of a Visitor pattern.
// findes the index and leaf IO for answering the query and prints
// the resulting data IDs to stdout.
class MyVisitor : public IVisitor
{
public:
	size_t m_indexIO;
	size_t m_leafIO;

private: 
	int m_count;

public:
	MyVisitor() : m_indexIO(0), m_leafIO(0) {m_count=0;}

	void visitNode(const INode& n)
	{
		if (n.isLeaf()) m_leafIO++;
		else m_indexIO++;
	}

	void visitData(const IData& d)
	{
		IShape* pS;
		d.getShape(&pS);
			// do something.
		delete pS;

		// data should be an array of characters representing a Region as a string.
		byte* pData = 0;
		uint32_t cLen = 0;
		d.getData(cLen, &pData);
		// do something.
		//string s = reinterpret_cast<char*>(pData);
		//cout << s << endl;
		delete[] pData;

		cout << "answer: "<<d.getIdentifier() << endl;
			// the ID of this data entry is an answer to the query. I will just print it to stdout.
	}

	void visitData(std::vector<const IData*>& v)
	{
		string sP = getDataString(v[0]);
		string sQ = getDataString(v[1]);
		Polygon_2 P,Q;
		parsePolygon(sP,P);
		parsePolygon(sQ,Q);
		double overlap_a = CGAL::to_double(overlap_area(P,Q));
		if (overlap_a==0) return;
		double union_a = CGAL::to_double(union_area(P,Q));
		Point_2 c1 = CGAL::centroid(P.vertices_begin(), P.vertices_end());
		Point_2 c2 = CGAL::centroid(Q.vertices_begin(), Q.vertices_end());
		double squared_dist = CGAL::to_double(CGAL::squared_distance(c1,c2));
		/*cout << "answer "<<m_count<<": "<<v[0]->getIdentifier() << " " << v[1]->getIdentifier() << endl;
		std::cout<<"overlap area: "<<overlap_area(P,Q)<<std::endl;
		std::cout<<"union area: "<<union_area(P,Q)<<std::endl;
		std::cout<<"centroid: "<<c1<<" | "<<c2<<std::endl;
		std::cout<<"area ratio: "<<overlap_area(P,Q)*1.0/union_area(P,Q)<<std::endl;
		std::cout<< "distance: "<<sqrt(squared_dist)<<std::endl;
		std::cout<<std::endl;*/
			m_count++;
			std::cout<<m_count<<'|'<<v[0]->getIdentifier() << "|" << v[1]->getIdentifier()<< "|"<<overlap_a/union_a<< "|"<<sqrt(squared_dist)<<"|"<<overlap_a<<"|"<<union_a<<"|"<<c1<<"|"<<c2<<std::endl;
			if (DEBUG) std::cout<<"P1="<<P<<"\nP2="<<Q<<std::endl;
	}
};


int main(int argc, char** argv)
{
	try
	{
		if (argc != 3)
		{
			std::cerr << "Usage: " << argv[0] << " tree_file1 tree_file2" << std::endl;
			return -1;
		}

		// second rtree
		std::string baseName2 = argv[2];

		IStorageManager* diskfile2 = StorageManager::loadDiskStorageManager(baseName2);
			// this will try to locate and open an already existing storage manager.

		StorageManager::IBuffer* file2 = StorageManager::createNewRandomEvictionsBuffer(*diskfile2, 10, false);
			// applies a main memory random buffer on top of the persistent storage manager
			// (LRU buffer, etc can be created the same way).


		// first rtree 
		std::string baseName1 = argv[1];

		IStorageManager* diskfile1 = StorageManager::loadDiskStorageManager(baseName1);
			// this will try to locate and open an already existing storage manager.

		StorageManager::IBuffer* file1 = StorageManager::createNewRandomEvictionsBuffer(*diskfile1, 10, false);
			// applies a main memory random buffer on top of the persistent storage manager
			// (LRU buffer, etc can be created the same way).

		// If we need to open an existing tree stored in the storage manager, we only
		// have to specify the index identifier as follows
		ISpatialIndex* tree1 = RTree::loadRTree(*file1, 1, *file2, 1);

		std::cerr << *tree1;
		std::cerr << "Buffer hits: " << file1->getHits() << std::endl;
		std::cerr << "Index ID: " << 1 << std::endl;

		bool ret1 = tree1->isIndexValid();
		if (ret1 == false) std::cerr << "ERROR: Structure is invalid!" << std::endl;
		else std::cerr << "The stucture seems O.K." << std::endl;


/*		// If we need to open an existing tree stored in the storage manager, we only
		// have to specify the index identifier as follows
		ISpatialIndex* tree2 = tree1->m_treeToJoin;

		std::cerr << *tree2;
		std::cerr << "Buffer hits: " << file2->getHits() << std::endl;
		std::cerr << "Index ID: " << 1 << std::endl;

		bool ret2 = tree2->isIndexValid();
		if (ret2 == false) std::cerr << "ERROR: Structure is invalid!" << std::endl;
		else std::cerr << "The stucture seems O.K." << std::endl;*/


		//spatial join of two rtrees
		MyVisitor vis;
		tree1->joinQuery(vis);

		
		delete tree1;
		delete file1;
		delete diskfile1;
			// delete the buffer first, then the storage manager
			// (otherwise the the buffer will fail trying to write the dirty entries).

/*		delete tree2; */
		delete file2;
		delete diskfile2;
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
