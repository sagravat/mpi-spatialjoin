#include <RTreeQuery.h>

using namespace SpatialIndex;
using namespace std;


int main(int argc, char** argv)
{
	try
	{
		if (argc != 2)
		{
			std::cerr << "Usage: " << argv[0] << " tree_file" << std::endl;
			return -1;
		}

		std::string baseName = argv[1];

		IStorageManager* diskfile = StorageManager::loadDiskStorageManager(baseName);
			// this will try to locate and open an already existing storage manager.

		StorageManager::IBuffer* file = StorageManager::createNewRandomEvictionsBuffer(*diskfile, 10, false);
			// applies a main memory random buffer on top of the persistent storage manager
			// (LRU buffer, etc can be created the same way).

		// If we need to open an existing tree stored in the storage manager, we only
		// have to specify the index identifier as follows
		ISpatialIndex* tree = RTree::loadRTree(*file, 1);

		std::cerr << *tree;
		std::cerr << "Buffer hits: " << file->getHits() << std::endl;
		std::cerr << "Index ID: " << 1 << std::endl;

		bool ret = tree->isIndexValid();
		if (ret == false) std::cerr << "ERROR: Structure is invalid!" << std::endl;
		else std::cerr << "The stucture seems O.K." << std::endl;


		//selfjoin
		MyVisitor vis;
		double plow[2]={0,0}, phigh[2]={1000000,1000000};
		Region r = Region(plow, phigh, 2);
		//std::cout<<"selfjoin: "<<std::endl;
		tree->selfJoinQuery(r, vis);

		
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
		return -1;
	}

	return 0;
}
