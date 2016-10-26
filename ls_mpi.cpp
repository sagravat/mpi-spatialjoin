#include <boost/mpi/environment.hpp>
#include <boost/mpi/communicator.hpp>
#include <boost/filesystem.hpp>
#include <iostream>
#include <fstream>
#include <iterator>
#include <algorithm>
#include <map>
#include <string>
#include <vector>
#include <RTreeLoad.h>
#include <boost/tuple/tuple.hpp>
#include <boost/tuple/tuple_io.hpp>
#include <boost/foreach.hpp>
#include <boost/tokenizer.hpp>
#include <boost/algorithm/string.hpp>

using namespace SpatialIndex;

namespace mpi = boost::mpi;
namespace fs = boost::filesystem;
using namespace std;
using namespace boost::filesystem;

typedef boost::tuple<string, string> tup;
typedef boost::tokenizer<boost::char_separator<char> > tokenizer;
    

std::vector<tup> get_files(string s);

    std::map<string,string> slide_hostmap;
    std::map<string,int> host_mastermap;



std::vector<tup> get_files(string dir) {
  path p (dir);   // p reads clearer than argv[1] in the following code
  std::vector<tup> files;

  try
  {
    if (exists(p))    // does p actually exist?
    {
      if (is_regular_file(p))        // is p a regular file?
        cout << p << " size is " << file_size(p) << '\n';

      else if (is_directory(p))      // is p a directory?
      {
        cout << p << " is a directory containing:\n";

        directory_iterator end_itr; // default construction yields past-the-end
          for ( directory_iterator itr( p );
                          itr != end_itr;
                                  ++itr )
                {
                    std::string file            =  itr->path().filename().string().c_str();
                    std::string relative_path   =  itr->path().parent_path().string().c_str();
                    tup myTuple(relative_path, file);
                    files.push_back(myTuple);
                }
      
      }
      else
        cout << p << " exists, but is neither a regular file nor a directory\n";
    }
    else
      cout << p << " does not exist\n";
  }

  catch (const filesystem_error& ex)
  {
    cout << ex.what() << '\n';
  }

  return files;

}

int route_file_to_node(string data, int rank) {

    typedef vector< string > split_vector_type;
    split_vector_type SplitVec;
    split_vector_type FileNameVec;
    split_vector_type KeyVec;
    boost::split( SplitVec, data, boost::is_any_of("|") );
    boost::split( FileNameVec, SplitVec[1], boost::is_any_of("-") );
    boost::split( KeyVec, FileNameVec[0], boost::is_any_of(".") );
    string key = KeyVec[0] + "." + KeyVec[1];
    string filename = SplitVec[0] + "/" + SplitVec[1];

    char hostname[255];
    gethostname(hostname,255);
    string host = slide_hostmap[key];
    return host_mastermap[host];
}

void init() {

    slide_hostmap["astroII.1"] = "node32.clus.cci.emory.edu";
    slide_hostmap["astroII.2"] = "node32.clus.cci.emory.edu";

    slide_hostmap["gbm0.1"] = "node33.clus.cci.emory.edu";
    slide_hostmap["gbm0.2"] = "node33.clus.cci.emory.edu";

    slide_hostmap["gbm1.1"] = "node34.clus.cci.emory.edu";
    slide_hostmap["gbm1.2"] = "node34.clus.cci.emory.edu";

    slide_hostmap["normal.2"] = "node36.clus.cci.emory.edu";
    slide_hostmap["normal.3"] = "node36.clus.cci.emory.edu";

    slide_hostmap["oligoastroII.1"] = "node37.clus.cci.emory.edu";
    slide_hostmap["oligoastroII.2"] = "node37.clus.cci.emory.edu";

    slide_hostmap["oligoastroIII.1"] = "node38.clus.cci.emory.edu";
    slide_hostmap["oligoastroIII.2"] = "node38.clus.cci.emory.edu";

    slide_hostmap["oligoII.1"] = "node39.clus.cci.emory.edu";
    slide_hostmap["oligoII.2"] = "node39.clus.cci.emory.edu";

    slide_hostmap["oligoIII.1"] = "node40.clus.cci.emory.edu";
    slide_hostmap["oligoIII.2"] = "node40.clus.cci.emory.edu";

    slide_hostmap["gbm2.1"] = "node41.clus.cci.emory.edu";
    slide_hostmap["gbm2.2"] = "node41.clus.cci.emory.edu";

    /*
    host_mastermap["node32.clus.cci.emory.edu"] = 1;
    host_mastermap["node33.clus.cci.emory.edu"] = 24;
    host_mastermap["node34.clus.cci.emory.edu"] = 48;
    host_mastermap["node36.clus.cci.emory.edu"] = 72;
    host_mastermap["node37.clus.cci.emory.edu"] = 88;
    host_mastermap["node38.clus.cci.emory.edu"] = 104;
    host_mastermap["node39.clus.cci.emory.edu"] = 120;
    host_mastermap["node40.clus.cci.emory.edu"] = 136;
    host_mastermap["node41.clus.cci.emory.edu"] = 152;
    */

    host_mastermap["node32.clus.cci.emory.edu"] = 1;
    host_mastermap["node33.clus.cci.emory.edu"] = 2;
    host_mastermap["node34.clus.cci.emory.edu"] = 3;
    host_mastermap["node36.clus.cci.emory.edu"] = 4;
    host_mastermap["node37.clus.cci.emory.edu"] = 5;
    host_mastermap["node38.clus.cci.emory.edu"] = 6;
    host_mastermap["node39.clus.cci.emory.edu"] = 7;
    host_mastermap["node40.clus.cci.emory.edu"] = 8;
    host_mastermap["node41.clus.cci.emory.edu"] = 9;
}

int main(int argc, char* argv[]) 
{
    mpi::environment env(argc, argv);
    mpi::communicator world;
    init();
    int job_number= 0;
    int num_jobs = 0;
    string data;
    if (world.rank() == 0) {
        std::vector<tup> files = get_files("/data2/Sanjay/tiles"); 
        num_jobs = files.size() - 1;
        cout << "num_jobs " << num_jobs << endl;
        //for (unsigned i = 0; i < world.size()-1; ++i) {
        for (unsigned i = 0; i < files.size(); ++i) {
            tup t = files[i];
            string data = t.get<0>() + "|" + t.get<1>();
            int destination = route_file_to_node(data, world.rank());
            cout << i << " " << data << " " << destination << endl;
            world.send(destination, 0, data);
            job_number++;

        }

        /*
        while (job_number < num_jobs) {
            mpi::status msg = world.probe();
            world.recv(msg.source(), msg.tag(), data);
            tup t = files[job_number];
            data = t.get<0>() + "|" + t.get<1>();
                
            world.send(msg.source(), 0, data);
            job_number++;
        }
        */
        for (unsigned i = 1; i < world.size(); ++i) {
            world.send(i, 1, "quit");
        }
    } 

    else {
        while (true) {

            string data;
            int tag;
            mpi::status msg = world.probe();
            //world.recv(msg.source(), msg.tag(), data);
            world.recv(0, msg.tag(), data);
            if (msg.tag() == 1) {
                return 1;
            } else {
                char hostname[255];
                gethostname(hostname,255);

                typedef vector< string > split_vector_type;
                split_vector_type SplitVec;
                split_vector_type FileNameVec;
                boost::split( SplitVec, data, boost::is_any_of("|") );
                boost::split( FileNameVec, SplitVec[1], boost::is_any_of("-") );
                string filename = SplitVec[0] + "/" + SplitVec[1];
                ifstream myReadFile;
                ofstream myOutFile;
                string line;
                myReadFile.open(filename.c_str());
                path p("/scratch/spatialjoin/tiles/" + FileNameVec[0]);
                if (!exists(p)) {
                    string dir = "/scratch/spatialjoin/tiles/" + FileNameVec[0];
                    cout << " make dir " << dir << endl;
                    mkdir(dir.c_str(), 0755);
                }
                string outfilename = "/scratch/spatialjoin/tiles/" + FileNameVec[0] + "/" + SplitVec[1];
                cout << world.rank() << " writing " << outfilename << " to " << hostname << endl;
                myOutFile.open(outfilename.c_str());
                if (myReadFile.is_open()) {
                    while (getline(myReadFile,line))
                    {
                        myOutFile <<  line << endl;
                    }
                    myReadFile.close();
                    myOutFile.close();
                 }

                //cout << data << " " << hostname << endl;
                //world.send(0, 0, "done");
            }

        }

    }
    return 0;
}

