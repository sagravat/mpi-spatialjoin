#include <boost/mpi/environment.hpp>
#include <boost/mpi/communicator.hpp>
#include <boost/filesystem.hpp>
#include <iostream>
#include <iterator>
#include <algorithm>
#include <string>
#include <vector>
#include <RTreeLoad.h>
#include <RTreeQuery.h>
//#include "../spatialindex/src/spatialindex/SpatialIndexImpl.h"
#include <boost/tuple/tuple.hpp>
#include <boost/tuple/tuple_io.hpp>
#include <boost/foreach.hpp>
#include <boost/tokenizer.hpp>
#include <boost/algorithm/string.hpp>
#include <unistd.h>
#include <stdio.h>

using namespace SpatialIndex;

namespace mpi = boost::mpi;
namespace fs = boost::filesystem;
using namespace std;
using namespace boost::filesystem;

typedef boost::tuple<string, string> tup;
typedef boost::tokenizer<boost::char_separator<char> > tokenizer;
//typedef Tools::PoolPointer<RTree::Node> NodePtr;
    
typedef struct {
    bool exists;
    string tile_id;
    vector<string> files;
} tile_result;

std::vector<tup> get_files(string s);
void load_index(string relative_path, string inputFile, string dir);
tile_result get_tile_result(string relative_path, string file_name); 


class MyDataStream : public IDataStream
{
public:
	MyDataStream(std::string inputFile) : m_pNext(0)
	{
		m_filename = inputFile;

		m_id=0;
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
		std::string line;

		if (m_fin.good())
		{
			getline(m_fin,line);
			if (line.length()==0) return;
			m_pNext = parseInputLine(line, m_filename, m_id);
		}
	}

	std::ifstream m_fin;
	string m_filename;
	RTree::Data* m_pNext;
	id_type m_id;
};

std::vector<string> get_dirs(string dir) {
  path p (dir);   // p reads clearer than argv[1] in the following code
  std::vector<string> dirs;

  try
  {
    if (exists(p))    // does p actually exist?
    {
      if (is_regular_file(p))        // is p a regular file?
        cout << p << " size is " << file_size(p) << '\n';

      else if (is_directory(p))      // is p a directory?
      {
        //cout << p << " is a directory containing:\n";
        
        directory_iterator end_itr; // default construction yields past-the-end
          for ( directory_iterator itr( p );
                          itr != end_itr;
                                  ++itr )
                {
                    std::string dir            =  itr->path().string().c_str();
                    dirs.push_back(dir);
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

  return dirs;

}
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
        //cout << p << " is a directory containing:\n";

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

void load_index(string relative_path, string file_name, string dir) {

    vector<string> trees;
    //vector<StorageManager::IBuffer*> trees;
	string joinFile = ""; 
	try
	{
        tile_result result = get_tile_result(relative_path, file_name); 
	    joinFile = "/scratch/spatialjoin/joins/" + dir + "/" + result.tile_id + ".txt";
        path q("/scratch/spatialjoin/joins/" + dir );
        if (!exists(q))
            create_directories(q);
        for (int i = 0; i < result.files.size(); i++) {

        //std::string inputFile = relative_path + "/" + file_name;
        string inputFile = result.files[i];
     
	//	std::string baseName = "/scratch/spatialjoin/indices/" + dir + "/" + file_name + ".treeFile";
        char index[10];
        sprintf(index,"%d",i);
		std::string baseName = "/scratch/spatialjoin/indices/" + dir + "/" + result.tile_id + ".treeFile."+index;
        path p("/scratch/spatialjoin/indices/" + dir );
        if (!exists(p))
            create_directories(p);
		double utilization = 0.7;
        int    capacity = 100;
		IStorageManager* diskfile = StorageManager::createNewDiskStorageManager(baseName, 4096);
			// Create a new storage manager with the provided base name and a 4K page size.

		StorageManager::IBuffer* file = StorageManager::createNewRandomEvictionsBuffer(*diskfile, 10, false);
			// applies a main memory random buffer on top of the persistent storage manager
			// (LRU buffer, etc can be created the same way).

		MyDataStream stream(inputFile.c_str());

		// Create and bulk load a new RTree with dimensionality 2, using "file" as
		// the StorageManager and the RSTAR splitting policy.
		id_type indexIdentifier;
		ISpatialIndex* tree = RTree::createAndBulkLoadNewRTree(
			RTree::BLM_STR, stream, *file, utilization, capacity, capacity, 2, SpatialIndex::RTree::RV_RSTAR, indexIdentifier);
        //trees.push_back(tree);
        trees.push_back(baseName);

        //ISpatialIndex* tree1 = RTree::loadRTree(*file, 1, *file, 1);
		//std::cerr << *tree;
		//std::cerr << "Buffer hits: " << file->getHits() << std::endl;
		//std::cerr << "Index ID: " << indexIdentifier << std::endl;
    
		bool ret = tree->isIndexValid();
		//if (ret == false) std::cerr << "ERROR: Structure is invalid!" << std::endl;
		//else std::cerr << "The stucture seems O.K." << std::endl;

		delete tree;
		delete file;
		delete diskfile;
			// delete the buffer first, then the storage manager
			// (otherwise the the buffer will fail trying to write the dirty entries).
        }
        IStorageManager* diskfile1 = StorageManager::loadDiskStorageManager(trees[0]);
        IStorageManager* diskfile2 = StorageManager::loadDiskStorageManager(trees[1]);

        StorageManager::IBuffer* file1 = StorageManager::createNewRandomEvictionsBuffer(*diskfile1, 10, false);
        StorageManager::IBuffer* file2 = StorageManager::createNewRandomEvictionsBuffer(*diskfile2, 10, false);

        ISpatialIndex* tree1 = RTree::loadRTree(*file1, 1, *file2, 1);
        //ISpatialIndex * t = trees[0];
        //NodePtr root1 = RTree::readNode(t->m_rootID);
        //NodePtr root2 = RTree::readNode(trees[0]->m_rootID);
        MyVisitor vis;
        tree1->joinQuery(vis, joinFile);
        //trees[0].joinQuery(root,trees[1], root2, vis);
        //for (int i = 0; i < trees.size(); i++)
            //delete(trees[i]);
        //void SpatialIndex::RTree::RTree::joinQuery(NodePtr n1, SpatialIndex::RTree::RTree* tree, NodePtr n2, IVisitor& vis)
	}
	catch (Tools::Exception& e)
	{
		std::cerr << "******ERROR******" << std::endl;
		std::string s = e.what();
		std::cerr << s << std::endl;
	}
}

tile_result get_tile_result(string dir, string tile_id) {
    tile_result result;
    result.exists = true;
    typedef vector< string > split_vector_type;
    split_vector_type SplitVec;
    split_vector_type FileNameVec;
    boost::split( SplitVec, dir, boost::is_any_of(".") );
    string stemdir = SplitVec[0] + "." + SplitVec[1] + "." + SplitVec[2] + "." + SplitVec[3];

    boost::split( SplitVec, tile_id, boost::is_any_of("-") );
    boost::split( FileNameVec, SplitVec[0], boost::is_any_of(".") );
    string tile_name = FileNameVec[0] + "." + FileNameVec[1] + "." + FileNameVec[2] + "." + FileNameVec[3];

    string tile_file1 = stemdir + ".1/" + tile_name + ".1-" + SplitVec[1] + "-" + SplitVec[2];
    string tile_file2 = stemdir + ".2/" + tile_name + ".2-" + SplitVec[1] + "-" + SplitVec[2];
    path p(tile_file2);
    vector<string> files;
    files.push_back(tile_file1);
    files.push_back(tile_file2);
    result.files = files;
    result.tile_id = SplitVec[1] + "-" + SplitVec[2];

    return result;
}

tile_result other_tile_exists(string dir, string tile_id) {
    tile_result result;
    result.exists = false;
    typedef vector< string > split_vector_type;
    split_vector_type SplitVec;
    split_vector_type FileNameVec;
    boost::split( SplitVec, dir, boost::is_any_of(".") );
    string stemdir = SplitVec[0] + "." + SplitVec[1] + "." + SplitVec[2] + "." + SplitVec[3];

    boost::split( SplitVec, tile_id, boost::is_any_of("-") );
    boost::split( FileNameVec, SplitVec[0], boost::is_any_of(".") );
    string tile_name = FileNameVec[0] + "." + FileNameVec[1] + "." + FileNameVec[2] + "." + FileNameVec[3];

    string tile_file1 = stemdir + ".1/" + tile_name + ".1-" + SplitVec[1] + "-" + SplitVec[2];
    string tile_file2 = stemdir + ".2/" + tile_name + ".2-" + SplitVec[1] + "-" + SplitVec[2];
    path p(tile_file2);
    vector<string> files;
    files.push_back(tile_file1);
    if (exists(p)) {
        result.exists = true;
        files.push_back(tile_file2);
    }
    result.files = files;
    result.tile_id = SplitVec[1] + "-" + SplitVec[2];

    return result;
}

int main(int argc, char* argv[]) 
{
    mpi::environment env(argc, argv);
    mpi::communicator world;
    std::map<string,string> map;

    char hostname[255];
    gethostname(hostname,255);
    //cout << world.rank() << " " << hostname << endl;


    long nprocs = sysconf(_SC_NPROCESSORS_ONLN);
    if (world.rank() == 0 || world.rank() == 24 || world.rank() == 48 || 
            world.rank() == 72 || world.rank() == 96 || world.rank() == 112 
            || world.rank() == 128 || world.rank() == 144 || world.rank() == 160) {
            //|| world.rank() == 176) {
        std::vector<string> dirs = get_dirs("/scratch/spatialjoin/tiles"); 
        char hostname[255];
        gethostname(hostname,255);


        int rank = world.rank();
        for (unsigned i = 0; i < dirs.size(); ++i) {
            typedef vector< string > split_vector_type;
            split_vector_type SplitVec;
            split_vector_type FileNameVec;
            boost::split( SplitVec, dirs[i], boost::is_any_of(".") );
            string stemdir = SplitVec[0] + "." + SplitVec[1] + "." + SplitVec[2] + "." + SplitVec[3];
            //cout << hostname << " " << stemdir << endl;
            map[stemdir] = stemdir;
        }
        std::map<string,string>::iterator it;
        for (it = map.begin(); it != map.end(); ++it) {
            string dir = it->first + ".1";
            cout << dir << endl;
            std::vector<tup> files = get_files(dir); 
            int num_jobs = files.size() - 1;
            int job_number = 0;
            int len = (nprocs > num_jobs ? num_jobs : nprocs);
            cout << " ************* num_jobs: " << num_jobs << " nprocs " << nprocs << " " <<
                 hostname << endl;
            for (unsigned i = 0; i < len; ++i) {
                tup t = files[i];
                string data = t.get<0>() + "|" + t.get<1>();
                string tile1 = t.get<0>() + "/" + t.get<1>();
                cout << data << " " << hostname << endl;
                tile_result result = other_tile_exists(t.get<0>(),t.get<1>());
                if (result.exists) {
                    //cout << "sending to " << (rank + job_number) << " " << result.files[1] << endl;
                    world.send((rank + job_number), 0, data);
                }
                //cout << "job number " << job_number << " out of " << num_jobs << " for " << rank << endl;
                job_number++;

            }
            while (job_number <= num_jobs) {
                mpi::status msg = world.probe();
                string data;
                world.recv(msg.source(), msg.tag(), data);
                tup t = files[job_number];
                data = t.get<0>() + "|" + t.get<1>();
                tile_result result = other_tile_exists(t.get<0>(),t.get<1>());
                if (result.exists) {
                    cout << "sending " << result.files[1] << endl;
                    world.send(msg.source(), 0, data);
                }
                cout << "job number " << job_number << " out of " << num_jobs << " for " << rank << endl;
                job_number++;
            }
        }
        cout << "**************** DONE WITH " << rank << " ******************* " << endl;
        for (unsigned i = 0; i < nprocs; ++i) {
            world.send(rank + i, 1, "quit");
        }

    } 
    
    else {
        while (true) {

            //tup data;
            string data;
            int tag;
            mpi::status msg = world.probe();
            world.recv(msg.source(), msg.tag(), data);
            char hostname[255];
            gethostname(hostname,255);
            if (msg.tag() == 1) {
                return 1;
            } else {
                typedef vector< string > split_vector_type;
                split_vector_type SplitVec;
                split_vector_type FileNameVec;
                boost::split( SplitVec, data, boost::is_any_of("|") );
                tile_result result = get_tile_result(SplitVec[0], SplitVec[1]);
                boost::split( FileNameVec, SplitVec[1], boost::is_any_of("-") );
                //cout << "\t " << hostname << " " << world.rank() << " received " << data << " from " << msg.source() << endl;
                load_index(SplitVec[0], SplitVec[1], FileNameVec[0]);
                cout << "rank " << world.rank() << " RECV " << hostname << " " << SplitVec[1] << endl;
                world.send(msg.source(), 0, "done");
            }

        }

    }
    return 1;
}

