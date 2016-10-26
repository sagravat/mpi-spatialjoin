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
    
typedef struct {
    bool exists;
    string tile_id;
    vector<string> files;
    string other_file;
    tup data2;
} tile_result;

tile_result other_tile_result(string relative_path, string file_name); 

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

    slide_hostmap["astroII.1"] = "node41.clus.cci.emory.edu";
    slide_hostmap["astroII.2"] = "node41.clus.cci.emory.edu";

    slide_hostmap["gbm0.1"] = "node33.clus.cci.emory.edu";
    slide_hostmap["gbm0.2"] = "node33.clus.cci.emory.edu";

    slide_hostmap["gbm1.1"] = "node34.clus.cci.emory.edu";
    slide_hostmap["gbm1.2"] = "node32.clus.cci.emory.edu";

    //slide_hostmap["normal.2"] = "node36.clus.cci.emory.edu";
    slide_hostmap["normal.2"] = "node33.clus.cci.emory.edu";
    slide_hostmap["normal.3"] = "node35.clus.cci.emory.edu";

    slide_hostmap["oligoastroII.1"] = "node37.clus.cci.emory.edu";
    slide_hostmap["oligoastroII.2"] = "node37.clus.cci.emory.edu";

    slide_hostmap["oligoastroIII.1"] = "node38.clus.cci.emory.edu";
    slide_hostmap["oligoastroIII.2"] = "node38.clus.cci.emory.edu";

    slide_hostmap["oligoII.1"] = "node39.clus.cci.emory.edu";
    slide_hostmap["oligoII.2"] = "node39.clus.cci.emory.edu";

    slide_hostmap["oligoIII.1"] = "node35.clus.cci.emory.edu";
    slide_hostmap["oligoIII.2"] = "node40.clus.cci.emory.edu";

    slide_hostmap["gbm2.1"] = "node32.clus.cci.emory.edu";
    slide_hostmap["gbm2.2"] = "node32.clus.cci.emory.edu";

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
    host_mastermap["node35.clus.cci.emory.edu"] = 4;
//    host_mastermap["node36.clus.cci.emory.edu"] = 5;
    host_mastermap["node37.clus.cci.emory.edu"] = 5;
    host_mastermap["node38.clus.cci.emory.edu"] = 6;
    host_mastermap["node39.clus.cci.emory.edu"] = 7;
    host_mastermap["node40.clus.cci.emory.edu"] = 8;
    host_mastermap["node41.clus.cci.emory.edu"] = 9;
}

tile_result other_tile_exists(string dir, string tile_id) {
    tile_result result;
    result.exists = false;
    typedef vector< string > split_vector_type;
    split_vector_type SplitVec;
    split_vector_type FileNameVec;
    boost::split( SplitVec, dir, boost::is_any_of("/") );
    string stemdir = SplitVec[0] + "/" + SplitVec[1] + "/" + SplitVec[2] + "/" + SplitVec[3] +"/";

    boost::split( SplitVec, tile_id, boost::is_any_of("-") );
    boost::split( FileNameVec, SplitVec[0], boost::is_any_of(".") );
    string tile_name = FileNameVec[0] + "." + FileNameVec[1] + "." + FileNameVec[2] + "." + FileNameVec[3];


    string tile_file1 = stemdir + "alg1/" + tile_name + ".1-" + SplitVec[1] + "-" + SplitVec[2];
    string tile_file2 = stemdir + "alg2/" + tile_name + ".2-" + SplitVec[1] + "-" + SplitVec[2];
    path p(tile_file2);
    vector<string> files;
    files.push_back(tile_file1);
    if (exists(p)) {
        result.exists = true;
        files.push_back(tile_file2);
        result.other_file = tile_file2;
        tup myTuple(stemdir + "alg2", tile_name + ".2-" + SplitVec[1] + "-" + SplitVec[2]);
        result.data2 = myTuple;
    }

    result.files = files;
    result.tile_id = SplitVec[1] + "-" + SplitVec[2];

    return result;
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
        std::vector<tup> files = get_files("/data2/Sanjay/tiles/alg1"); 
        num_jobs = files.size() - 1;
        cout << "num_jobs " << num_jobs << endl;
        double sum_file_size = 0;
        //for (unsigned i = 0; i < world.size()-1; ++i) {
        int destination = 1;
        int matches = 0;
        for (unsigned i = 0; i < files.size(); ++i) {
            tup t = files[i];
            string data = t.get<0>() + "|" + t.get<1>();
            string file = t.get<0>() + "/" + t.get<1>();
            tile_result result = other_tile_exists(t.get<0>(), t.get<1>());
            if (result.exists) {
                matches++;
                sum_file_size += file_size(file);
                string file2 = result.data2.get<0>() + "/" + result.data2.get<1>();
                string data2 = result.data2.get<0>() + "|" + result.data2.get<1>();
                sum_file_size += file_size(file2);

                    //cout << i << " " << data << " to " << destination << endl;
                //int destination = route_file_to_node(data, world.rank());

                if (sum_file_size > 1627838992) {
                    sum_file_size = 0;
                    destination++;
                    cout << destination <<  " job " << job_number << " " << files.size() << endl;
                }
                world.send(destination, 0, data);
                world.send(destination, 0, data2);
            }
            job_number++;

        }

        //cout.setf(ios::fixed);
        //cout << " sum " << sum_file_size << endl;
        //cout << " sum/2 " << sum_file_size/2.0 << endl;
        //cout << " sum/9 " << sum_file_size/9.0 << endl;

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
                //cout << world.rank() << " writing " << outfilename << " to " << hostname << 
                    //" filesize = " << file_size(filename.c_str()) << endl;
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

