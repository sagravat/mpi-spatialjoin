#include <boost/mpi/environment.hpp>
#include <boost/mpi/communicator.hpp>
#include <boost/filesystem.hpp>
#include <iostream>
#include <iterator>
#include <algorithm>
#include <string>
#include <vector>
#include <RTreeQuery.h>
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
void join(string relative_path, string inputFile);


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

void join(string relative_path, string file_name) {
	try
	{

		// second rtree
		std::string baseName2 = relative_path + "/algorithm2/" + file_name+".2";

		IStorageManager* diskfile2 = StorageManager::loadDiskStorageManager(baseName2);
			// this will try to locate and open an already existing storage manager.

        StorageManager::IBuffer* file2 = StorageManager::createNewRandomEvictionsBuffer(*diskfile2, 10, false);
			// applies a main memory random buffer on top of the persistent storage manager
			// (LRU buffer, etc can be created the same way).

		// first rtree 
		std::string baseName1 = relative_path + "/algorithm1/" + file_name+ ".1";

		IStorageManager* diskfile1 = StorageManager::loadDiskStorageManager(baseName1);
			// this will try to locate and open an already existing storage manager.

		StorageManager::IBuffer* file1 = StorageManager::createNewRandomEvictionsBuffer(*diskfile1, 10, false);
			// applies a main memory random buffer on top of the persistent storage manager
			// (LRU buffer, etc can be created the same way).

		// If we need to open an existing tree stored in the storage manager, we only
		// have to specify the index identifier as follows
		ISpatialIndex* tree1 = RTree::loadRTree(*file1, 1, *file2, 1);

		//std::cerr << *tree1;
		//std::cerr << "Buffer hits: " << file1->getHits() << std::endl;
		//std::cerr << "Index ID: " << 1 << std::endl;

		bool ret1 = tree1->isIndexValid();
		if (ret1 == false) std::cerr << "ERROR: Structure is invalid!" << std::endl;
		else std::cerr << "The stucture seems O.K." << std::endl;


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
	}
	catch (Tools::Exception& e)
	{
		std::cerr << "******ERROR******" << std::endl;
		std::string s = e.what();
		std::cerr << s << std::endl;
	}

}


int main(int argc, char* argv[]) 
{
    mpi::environment env(argc, argv);
    mpi::communicator world;
    int job_number= 1;
    int num_jobs = 0;
    if (world.rank() == 0) {
        std::vector<tup> files = get_files("/data3/spatialjoin/indices/algorithm1"); 
        num_jobs = files.size();
        int limit = (num_jobs < world.size() ? num_jobs : world.size());
        for (unsigned i = 0; i < limit; ++i) {
            tup t = files[i];
            string data = t.get<0>() + "|" + t.get<1>();
            cout << "sent " << t.get<1>() << " to " << job_number << " out of " << num_jobs << endl;
            world.send(job_number, 0, data);
            job_number++;

        }

        while (job_number < num_jobs) {
            mpi::status msg = world.probe();
            string data;
            world.recv(msg.source(), msg.tag(), data);
            tup t = files[job_number];
            data = t.get<0>() + "|" + t.get<1>();
            cout << "sent " << t.get<1>() << " to " << msg.source() << " for job " << job_number << " out of " << num_jobs << endl;
            //cout << "sent " << t.get<1>() << " to " << msg.source() << " for job " << job_number << endl;
            world.send(msg.source(), 0, data);
            job_number++;


        }
        for (unsigned i = 1; i < world.size(); ++i) {
            world.send(i, 1, "quit");
        }
    } else {
        while (true) {

            //tup data;
            string data;
            int tag;
            mpi::status msg = world.probe();
            //world.recv(msg.source(), msg.tag(), data);
            world.recv(0, msg.tag(), data);
            if (msg.tag() == 1) {
                return 1;
            } else {
                typedef vector< string > split_vector_type;
                split_vector_type SplitVec;
                split_vector_type FileVec;
                boost::split( SplitVec, data, boost::is_any_of("|") );
                boost::split( FileVec, SplitVec[1], boost::is_any_of(".") );
                string file_root_name = FileVec[0] + "." + FileVec[1] + "." +  FileVec[2] + "." + FileVec[3];
                cout << file_root_name << endl;
                world.send(0, 0, "done");
                /*
                boost::char_separator<char> sep("|");
                tokenizer tokens(data, sep);
                BOOST_FOREACH(string t, tokens)
                {
                    cout << t << "\t";
                }
                */
            }

        }

    }
    return 0;
}

