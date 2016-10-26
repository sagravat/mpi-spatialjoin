#include <RTreeQuery.h>

using namespace SpatialIndex;
using namespace std;


bool endsWith(const string& a, const string& b) {
    if (b.size() > a.size()) return false;
    return std::equal(a.begin() + a.size() - b.size(), a.end(), b.begin());
}

int main(int argc, char** argv)
{
	try
	{
		if (argc != 3)
		{
			std::cerr << "Usage: " << argv[0] << " treeDir1 treeDir2" << std::endl;
			return -1;
		}

		std::string tree_dir1 = argv[1];
		std::string tree_dir2 = argv[2];

                DIR *dp = opendir( tree_dir1.c_str() );
                if (dp == NULL) {
                        cerr << "Error(" << errno << ") opening " << tree_dir1 << endl;
                        return false;
                }

                ifstream fin;
                struct dirent *dirp;
                struct stat filestat;
		MyVisitor vis;
                while ((dirp = readdir( dp ))) {
			if (!endsWith(dirp->d_name,".idx")) continue;
                        std::string tree_file1 = tree_dir1 + "/" + dirp->d_name;
                        // If the file is a directory (or is in some way invalid) we'll skip it 
                        if (stat( tree_file1.c_str(), &filestat )) continue;
                        if (S_ISDIR( filestat.st_mode ))         continue;

			tree_file1 = tree_file1.substr(0,tree_file1.length()-4);
                        std::string tree_file2 = tree_dir2 + "/" + dirp->d_name;
			tree_file2 = tree_file2.substr(0,tree_file2.length()-4);

			std::cerr<<tree_file1<<' '<<tree_file2<<endl;


                        // retrieve the rtree for tree_file2 in tree_dir2
			IStorageManager* diskfile2 = StorageManager::loadDiskStorageManager(tree_file2);
			StorageManager::IBuffer* file2 = StorageManager::createNewRandomEvictionsBuffer(*diskfile2, 10, false);

			// retrieve the rtree for tree_file1
			IStorageManager* diskfile1 = StorageManager::loadDiskStorageManager(tree_file1);
			StorageManager::IBuffer* file1 = StorageManager::createNewRandomEvictionsBuffer(*diskfile1, 10, false);

			ISpatialIndex* tree1 = RTree::loadRTree(*file1, 1, *file2, 1);

			std::cerr << *tree1;
			std::cerr << "Buffer hits: " << file1->getHits() << std::endl;
			std::cerr << "Index ID: " << 1 << std::endl;

			bool ret1 = tree1->isIndexValid();
			if (ret1 == false) std::cerr << "ERROR: Structure is invalid!" << std::endl;
			else std::cerr << "The stucture seems O.K." << std::endl;

			//spatial join of two rtrees
			tree1->joinQuery(vis);
		
			delete tree1;
			delete file1;
			delete diskfile1;
			// delete the buffer first, then the storage manager
			// (otherwise the the buffer will fail trying to write the dirty entries).

			/* delete tree2; */
			delete file2;
			delete diskfile2;
			// delete the buffer first, then the storage manager
			// (otherwise the the buffer will fail trying to write the dirty entries).
                }

                closedir( dp );

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
