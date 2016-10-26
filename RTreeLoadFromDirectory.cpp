#include <RTreeLoad.h>

using namespace SpatialIndex;
using namespace std;

class MyDataStream : public IDataStream
{
public:
	MyDataStream(std::string inputDir) : m_pNext(0)
	{
		m_inputDir = inputDir;
		struct stat filestat;
                if (stat( inputDir.c_str(), &filestat )) 
		        throw Tools::IllegalArgumentException("Input file invalid.");

		string inputFile;
                if (S_ISDIR( filestat.st_mode )) {
			// the file is a directory
			m_dp = opendir( inputDir.c_str() );
                	while ((m_dirp = readdir( m_dp ))) {
		        	inputFile = m_inputDir + "/" + m_dirp->d_name;
				if (stat( inputFile.c_str(), &filestat )) continue;
				if (S_ISDIR( filestat.st_mode )) continue;
				break;
			}
			if (!m_dirp)
				throw Tools::IllegalArgumentException("Input directory empty.");
		} else {
			// the file is a regular file
			m_dp = 0;
		}

		m_id=0;
		m_fin.open(inputFile.c_str());
		m_filename = inputFile;
		cerr<<"read first file: "<<inputFile<<endl;

		if (! m_fin)
			throw Tools::IllegalArgumentException("Input file not found.");

		readNextEntry();
	}

	virtual ~MyDataStream()
	{
		m_fin.close();
		if (m_dp != 0) closedir(m_dp);
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
			if (line.length()>0) {
				//cerr << m_id<<endl;
				m_pNext = parseInputLine(line, m_filename, m_id);
				return;
			}
		}

		if (m_dp!=0) {
			// input is a directory, so we need to read the next file
			string inputFile;
			struct stat filestat;
                	while ((m_dirp = readdir( m_dp ))) {
		        	inputFile = m_inputDir + "/" + m_dirp->d_name;
				if (stat( inputFile.c_str(), &filestat )) continue;
				if (S_ISDIR( filestat.st_mode )) continue;
				break;
			}
			if (!m_dirp)
				return;
			cerr<<"read next file: "<<inputFile<<endl;

			m_fin.close();
			m_fin.open(inputFile.c_str());
			m_id = 0;
			m_filename = inputFile;
			if (! m_fin) {
				cerr<<" error here ?"<<endl;
				throw Tools::IllegalArgumentException("Input file not found.");
			}

			readNextEntry();
		}
	}

	std::ifstream m_fin;
	RTree::Data* m_pNext;
	string m_inputDir;
	string m_filename;
	id_type m_id;
	struct dirent *m_dirp;
	DIR *m_dp;
};

bool createSingleRTree(std::string input_file, std::string tree_file, int capacity, double utilization) {


                IStorageManager* diskfile = StorageManager::createNewDiskStorageManager(tree_file, 4096);
                        // Create a new storage manager with the provided base name and a 4K page size.

                StorageManager::IBuffer* file = StorageManager::createNewRandomEvictionsBuffer(*diskfile, 10,     false);
                        // applies a main memory random buffer on top of the persistent storage manager
                        // (LRU buffer, etc can be created the same way).

                MyDataStream stream(input_file);

                // Create and bulk load a new RTree with dimensionality 2, using "file" as
                // the StorageManager and the RSTAR splitting policy.
                id_type indexIdentifier;
                ISpatialIndex* tree = RTree::createAndBulkLoadNewRTree(
                        RTree::BLM_STR, stream, *file, utilization, capacity, capacity, 2, SpatialIndex::RTree::  RV_RSTAR, indexIdentifier);

                std::cerr << *tree;
                std::cerr << "Buffer hits: " << file->getHits() << std::endl;
                std::cerr << "Index ID: " << indexIdentifier << std::endl;

                bool ret = tree->isIndexValid();
                if (ret == false) std::cerr << "ERROR: Structure is invalid!" << std::endl;
                else std::cerr << "The stucture seems O.K." << std::endl;

                // delete the buffer first, then the storage manager
                // (otherwise the the buffer will fail trying to write the dirty entries).
                delete tree;
                delete file;
                delete diskfile;

                return ret;
}



int main(int argc, char** argv)
{
	try
	{
		if (argc != 5)
		{
			std::cerr << "Usage: " << argv[0] << " inputDir treeFile capacity(default=100) utilization(default=0.7)." << std::endl;
			return -1;
		}

		if (!createSingleRTree(argv[1],argv[2],atoi(argv[3]),atof(argv[4])))
			exit(1);
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
