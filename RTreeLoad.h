#include <CGAL/Exact_predicates_exact_constructions_kernel.h>
#include <CGAL/Boolean_set_operations_2.h>
#include <list>

#include <CGAL/Cartesian.h>
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


std::string splitFileName(std::string str) {
        size_t found;
        cout << "Splitting: " << str << endl;
        found=str.find_last_of("/\\");
        cout << " folder: " << str.substr(0,found) << endl;
        cout << " file: " << str.substr(found+1) << endl;
        return str.substr(found+1);
}

RTree::Data* parseInputLine(string line, string m_filename, id_type &m_id) {
                double low[2], high[2];
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

                        CGAL::Bbox_2 mbr = P.bbox();
                        low[0] = CGAL::to_double(mbr.xmin());
                        low[1] = CGAL::to_double(mbr.ymin());
                        high[0] = CGAL::to_double(mbr.xmax());
                        high[1] = CGAL::to_double(mbr.ymax());

                        Region r(low, high, 2);

                        std::ostringstream os;
                        // the polygon is stored using delta x and delta y in the rtree leaf node.
                        os<<m_filename<<'_'<<m_id<<' ';
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
                        return new RTree::Data(data.size()+1, reinterpret_cast<const byte*>(data.c_str()), r,     m_id++);
                                // Associate a bogus data array with every entry for testing purposes.
                                // Once the data array is given to RTRee:Data a local copy will be created.
                                // Hence, the input data array can be deleted after this operation if not
                                // needed anymore.
}





