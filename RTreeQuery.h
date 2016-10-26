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

#include <dirent.h>
#include <unistd.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <fstream>


const int DEBUG=0;

std::string getDataString(const IData* d) {
        byte* pData = 0;
        uint32_t cLen = 0;
        d->getData(cLen, &pData);
        string s = reinterpret_cast<char*>(pData);
        delete[] pData;
        return s;
}

void parsePolygon(std::string str,Polygon_2 &P,string &nameP) {
        std::istringstream iss2(str);
        int n;
        double x,y,lastx,lasty;
	iss2>>nameP;
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
	//cerr<<"before calling CGAL::intersection: \nP="<<P<<"\nQ="<<Q<<endl;
        CGAL::intersection(P, Q, std::back_inserter(overlap));
	//cerr<<"after calling CGAL::intersection"<<endl;
	//exit(1);
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


// example of a Visitor pattern.
// findes the index and leaf IO for answering the query and prints
// the resulting data IDs to stdout.
class MyVisitor  : public IVisitor
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
                //cerr<<m_count<<'|'<<v[0]->getIdentifier() << "|" << v[1]->getIdentifier()<<endl;
                string sP = getDataString(v[0]);
                string sQ = getDataString(v[1]);
                Polygon_2 P,Q;
                string nameP,nameQ;
                parsePolygon(sP,P,nameP);
                parsePolygon(sQ,Q,nameQ);
                double overlap_a = CGAL::to_double(overlap_area(P,Q));
                if (overlap_a==0) return;
                double union_a = CGAL::to_double(union_area(P,Q));
                //Point_2 c1;
                //Point_2 c2;
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
                std::cout<<m_count<<'|'<<nameP<<'|'<<nameQ<<'|'<<v[0]->getIdentifier() << "|" << v[1]->getIdentifier()<< "|"<<overlap_a/union_a << "|"<<sqrt(squared_dist) << "|"<<overlap_a << "|"<< union_a <<"|"<<c1 <<"|"<<c2<<std::endl;
                if (DEBUG) std::cout<<"P1="<<P<<"\nP2="<<Q<<std::endl;
        }

        void visitData(std::vector<const IData*>& v, std::string joinFile)
        {   
                //cerr<<m_count<<'|'<<v[0]->getIdentifier() << "|" << v[1]->getIdentifier()<<endl;
                string sP = getDataString(v[0]);
                string sQ = getDataString(v[1]);
                Polygon_2 P,Q;
                string nameP,nameQ;
                parsePolygon(sP,P,nameP);
                parsePolygon(sQ,Q,nameQ);
                double overlap_a = CGAL::to_double(overlap_area(P,Q));
                if (overlap_a==0) return;
                double union_a = CGAL::to_double(union_area(P,Q));
                //Point_2 c1;
                //Point_2 c2;
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
                ofstream myfile;
                myfile.open(joinFile.c_str(), ios::out | ios::app);
                myfile <<m_count<<'|'<<nameP<<'|'<<nameQ<<'|'<<v[0]->getIdentifier() << "|" << v[1]->getIdentifier()<< "|"<<overlap_a/union_a << "|"<<sqrt(squared_dist) << "|"<<overlap_a << "|"<< union_a <<"|"<<c1 <<"|"<<c2<<std::endl;
                myfile.close();
                if (DEBUG) std::cout<<"P1="<<P<<"\nP2="<<Q<<std::endl;
        }
};


