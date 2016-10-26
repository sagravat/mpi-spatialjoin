#include <RTreeLoad.h>


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
		double low[2], high[2];
		std::string line;

		if (m_fin.good())
		{
			getline(m_fin,line);
			if (line.length()==0) return;
			m_pNext = parseInputLine(line, m_filename, m_id);
		}
	}

	std::ifstream m_fin;
	RTree::Data* m_pNext;
	id_type m_id;
	string m_filename;
};

bool createSingleRTree(std::string input_file, std::string tree_file, int capacity, double utilization) {


		IStorageManager* diskfile = StorageManager::createNewDiskStorageManager(tree_file, 4096);
			// Create a new storage manager with the provided base name and a 4K page size.

		StorageManager::IBuffer* file = StorageManager::createNewRandomEvictionsBuffer(*diskfile, 10, false);
			// applies a main memory random buffer on top of the persistent storage manager
			// (LRU buffer, etc can be created the same way).

		MyDataStream stream(input_file);

		// Create and bulk load a new RTree with dimensionality 2, using "file" as
		// the StorageManager and the RSTAR splitting policy.
		id_type indexIdentifier;
		ISpatialIndex* tree = RTree::createAndBulkLoadNewRTree(
			RTree::BLM_STR, stream, *file, utilization, capacity, capacity, 2, SpatialIndex::RTree::RV_RSTAR, indexIdentifier);

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

bool createMultipleRTrees(std::string input_dir, std::string tree_dir, int capacity, double utilization) {

		DIR *dp = opendir( input_dir.c_str() );
		if (dp == NULL) {
    			cout << "Error(" << errno << ") opening " << input_dir << endl;
    			return false;
    		}

		ifstream fin;
		struct dirent *dirp;
		struct stat filestat;
  		while ((dirp = readdir( dp ))) {
			std::string input_file = input_dir + "/" + dirp->d_name;
			std::string tree_file = tree_dir + "/" + dirp->d_name;

    			// If the file is a directory (or is in some way invalid) we'll skip it 
    			if (stat( input_file.c_str(), &filestat )) continue;
    			if (S_ISDIR( filestat.st_mode ))         continue;

			// create a rtree for each file
			if (!createSingleRTree(input_file,tree_file,capacity,utilization))
				return false;
    		}

  		closedir( dp );	

		return true;
}

int main(int argc, char** argv)
{
	try
	{
		if (argc != 5)
		{
			std::cerr << "Usage: " << argv[0] << " inputDir treeDir capacity(default=100) utilization(default=0.7)." << std::endl;
			return -1;
		}

		if (!createMultipleRTrees(argv[1],argv[2],atoi(argv[3]),atof(argv[4])))
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
