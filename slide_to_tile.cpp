#include <boost/mpi/environment.hpp>
#include <boost/mpi/communicator.hpp>
#include <boost/filesystem.hpp>
#include <iostream>
#include <fstream>
#include <iterator>
#include <algorithm>
#include <string>
#include <vector>
#include <boost/tuple/tuple.hpp>
#include <boost/tuple/tuple_io.hpp>
#include <boost/foreach.hpp>
#include <boost/tokenizer.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/unordered_map.hpp>
#include <map>


namespace mpi = boost::mpi;
using namespace std;
using namespace boost::filesystem;

typedef boost::tuple<string, string> tup;
typedef boost::tokenizer<boost::char_separator<char> > tokenizer;
    

std::vector<tup> get_files(string s);

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



int main(int argc, char* argv[]) 
{
    mpi::environment env(argc, argv);
    mpi::communicator world;
    int job_number= 1;
    int num_jobs = 0;
    if (world.rank() == 0) {
        std::vector<tup> files = get_files("/data2/Sanjay/slides"); 
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
                boost::split( SplitVec, data, boost::is_any_of("|") );
                string rel_filename = SplitVec[1];
                string abs_filename = SplitVec[0] + "/" + SplitVec[1];
                ifstream in(abs_filename.c_str());
                if (!in.is_open()) return 1;
                vector< string > vec;
                string line;
                //typedef boost::unordered_map<std::string, string> map;
                
                std::map<string, string> tile_id_map;
                string filename = "";
                ofstream myfile;
                bool has_written = false;
                while (getline(in,line))
                {

                    boost::split( SplitVec, line, boost::is_any_of(",") );
                    if (tile_id_map.find(SplitVec[0]) == tile_id_map.end()) {
                        myfile.close();
                        tile_id_map[SplitVec[0]] = SplitVec[0];
                        boost::split( SplitVec, SplitVec[0], boost::is_any_of("-") );
                        filename = SplitVec[1] + "-" + SplitVec[2];
                        cout << filename << endl;
                        string ofilename = "/data2/Sanjay/tiles/" + rel_filename + "-" + filename;
                        myfile.open(ofilename.c_str());
                    }
                    myfile <<  line << endl;
                }
                in.close();

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

