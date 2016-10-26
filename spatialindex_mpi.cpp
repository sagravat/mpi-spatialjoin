#include <boost/mpi/environment.hpp>
#include <boost/mpi/communicator.hpp>
#include <boost/filesystem.hpp>
#include <iostream>
#include <iterator>
#include <algorithm>
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
void load_index(string relative_path, string inputFile, string dir);


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

void load_index(string relative_path, string file_name, string dir) {

	try
	{
        std::string inputFile = relative_path + "/" + file_name;
        cout << "inputFile: " << inputFile << endl;
		std::string baseName = "/scratch/spatialjoin/indices/" + dir + "/" + file_name + ".treeFile";
        path p("/scratch/spatialjoin/indices/" + dir + "/");
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

		//std::cerr << *tree;
		//std::cerr << "Buffer hits: " << file->getHits() << std::endl;
		//std::cerr << "Index ID: " << indexIdentifier << std::endl;

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
	}
}


int main(int argc, char* argv[]) 
{
    mpi::environment env(argc, argv);
    mpi::communicator world;
    int job_number= 0;
    int num_jobs = 0;
    if (world.rank() == 0) {
        std::vector<tup> files = get_files("/data2/Sanjay/tiles"); 
        num_jobs = files.size() - 1;
        for (unsigned i = 0; i < world.size()-1; ++i) {
            tup t = files[i];
            string data = t.get<0>() + "|" + t.get<1>();
            cout << "sent " << t.get<1>() << " for job " << job_number << " out of " << num_jobs << endl;
            world.send(i+1, 0, data);
            job_number++;

        }

        while (job_number <= num_jobs) {
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
        cout << " done with jobs " << endl;
        for (unsigned i = 1; i < world.size(); ++i) {
            cout << "terminate " << i << endl;
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
                cout << world.rank() << " received termination" << endl;
                return 1;
            } else {
                typedef vector< string > split_vector_type;
                split_vector_type SplitVec;
                split_vector_type FileNameVec;
                boost::split( SplitVec, data, boost::is_any_of("|") );
                boost::split( FileNameVec, SplitVec[1], boost::is_any_of("-") );
                //load_index(SplitVec[0], SplitVec[1], FileNameVec[0]);
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
    return 1;
}

