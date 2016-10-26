#include <RTreeQuery.h>
#include <vector>

using namespace SpatialIndex;
using namespace std;

int main(int argc, char** argv)
{
	try
	{
		if (argc != 3)
		{
			std::cerr << "Usage: " << argv[0] << " tree_file1 tree_file2" << std::endl;
			return -1;
		}
        vector<StorageManager::IBuffer*> trees;
		// second rtree
		std::string baseName2 = argv[2];

		IStorageManager* diskfile2 = StorageManager::loadDiskStorageManager(baseName2);
			// this will try to locate and open an already existing storage manager.

		StorageManager::IBuffer* file2 = StorageManager::createNewRandomEvictionsBuffer(*diskfile2, 10, false);
			// applies a main memory random buffer on top of the persistent storage manager
			// (LRU buffer, etc can be created the same way).


		// first rtree 
		std::string baseName1 = argv[1];

		IStorageManager* diskfile1 = StorageManager::loadDiskStorageManager(baseName1);
			// this will try to locate and open an already existing storage manager.

		StorageManager::IBuffer* file1 = StorageManager::createNewRandomEvictionsBuffer(*diskfile1, 10, false);
        trees.push_back(file1);
        trees.push_back(file2);
			// applies a main memory random buffer on top of the persistent storage manager
			// (LRU buffer, etc can be created the same way).

		// If we need to open an existing tree stored in the storage manager, we only
		// have to specify the index identifier as follows
		//ISpatialIndex* tree1 = RTree::loadRTree(*file1, 1, *file2, 1);
		ISpatialIndex* tree1 = RTree::loadRTree(*trees[0], 1, *trees[1], 1);

		std::cerr << *tree1;
		std::cerr << "Buffer hits: " << file1->getHits() << std::endl;
		std::cerr << "Index ID: " << 1 << std::endl;

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
		return -1;
	}

	return 0;
}
