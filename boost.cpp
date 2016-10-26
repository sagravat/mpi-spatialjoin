#include <boost/mpi/environment.hpp>
#include <boost/mpi/communicator.hpp>
#include <iostream>
#include <iterator>
#include <algorithm>
#include <string>
//#include <boost/filesystem.hpp>

namespace mpi = boost::mpi;
using namespace std;
//using namespace boost::filesystem;


int main(int argc, char* argv[]) 
{
    mpi::environment env(argc, argv);
    mpi::communicator world;
    cout << "I am process " << world.rank() << " of " << world.size()
        << "." << endl;
    return 0;
}

