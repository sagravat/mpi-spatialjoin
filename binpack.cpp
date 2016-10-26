#include <vector>
#include <iterator>
#include <iostream>
#include <boost/tuple/tuple.hpp>
#include <boost/tuple/tuple_io.hpp>
#include <boost/tokenizer.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <limits>

using namespace boost::filesystem;
using namespace std;

typedef boost::tuple<string, string> tup;
typedef boost::tuple<string, string, int> tup_filesize;

typedef struct {
    std::vector<tup_filesize> items;
    double total_weight;
} bin;

typedef struct {
    bool exists;
    string tile_id;
    vector<string> files;
    string other_file;
    tup data2;
} tile_result;


tile_result other_tile_exists(string dir, string tile_id) {
    tile_result result;
    result.exists = false;
    typedef vector< string > split_vector_type;
    split_vector_type SplitVec;
    split_vector_type FileNameVec;
    boost::split( SplitVec, dir, boost::is_any_of("/") );
    string stemdir = SplitVec[0] + "/" + SplitVec[1] + "/" + SplitVec[2] + "/" + SplitVec[3] +"/";

    boost::split( SplitVec, tile_id, boost::is_any_of("-") );
    boost::split( FileNameVec, SplitVec[0], boost::is_any_of(".") );
    string tile_name = FileNameVec[0] + "." + FileNameVec[1] + "." + FileNameVec[2] + "." + FileNameVec[3];


    string tile_file1 = stemdir + "alg1/" + tile_name + ".1-" + SplitVec[1] + "-" + SplitVec[2];
    string tile_file2 = stemdir + "alg2/" + tile_name + ".2-" + SplitVec[1] + "-" + SplitVec[2];
    path p(tile_file2);
    vector<string> files;
    files.push_back(tile_file1);
    if (exists(p)) {
        result.exists = true;
        files.push_back(tile_file2);
        result.other_file = tile_file2;
        tup myTuple(stemdir + "alg2", tile_name + ".2-" + SplitVec[1] + "-" + SplitVec[2]);
        result.data2 = myTuple;
    }

    result.files = files;
    result.tile_id = SplitVec[1] + "-" + SplitVec[2];

    return result;
}

std::vector<tup> get_files(std::string dir) {
  path p (dir);   
  std::vector<tup> files;

  try
  {
    if (exists(p))    
    {
      if (is_regular_file(p))        
        cout << p << " size is " << file_size(p) << '\n';

      else if (is_directory(p))      
      {
        cout << p << " is a directory containing:\n";

        directory_iterator end_itr; 
          for ( directory_iterator itr( p );
                          itr != end_itr;
                                  ++itr )
                {
                    std::string file            =  itr->path().filename().string().c_str();
                    std::string relative_path   =  itr->path().parent_path().string().c_str();
                    //std::string abs_file = relative_path + "/" + file;
                    //path p(abs_file);
                    //tup myTuple(abs_file, file_size(p));
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

double get_total_weight(std::vector<int> v) {
    std::vector<int>::iterator it;
    double total = 0;
    for ( it=v.begin() ; it < v.end(); it++ )
        total += *it;

    std::cout << "total: " << total << std::endl;
    return total;

}

int get_lightest_bin(bin bins[9]) {
    double min_weight = numeric_limits<double>::max();
    int index = 0;
    for (int i = 0; i < 9; i++) {
        if (bins[i].total_weight < min_weight) {
            min_weight = bins[i].total_weight;
            index = i;
        }

    }

    return index;

}
struct my_compare_op 
{ 
    bool operator()(const tup_filesize& a, const tup_filesize& b) const 
    { 
        return boost::get<2>(a) < boost::get<2>(b); 
    } 
};


void printBins(bin bins[9],int max_items ) 
{ 
    for (int j=0;j<max_items;j++) 
    { 
        printf("%1.3d: ", j);
        for (int i=0;i<9;i++) 
        { 
            vector<tup_filesize> v = bins[i].items;
            if (v.size() > j) {
                tup_filesize t = v[j];
                printf(" %1.8d",t.get<2>()); 
            } else {
                printf(" %1.8d",0); 
            }
        } 
        printf("\n"); 
    } 
    /*
    printf("-----------------------------------------------------------------------------------\n");
    for (int i=0;i<9;i++) 
    { 
        printf(" %14.0f", bins[i].total_weight);
    }
    printf("\n\n");
    */
}

int main ( int argc, int *argv ) {
    bin bins[9];
    for (int i = 0; i < 9; i++ ) {
        bins[i].total_weight = 0;
    }

    std::vector<tup> v = get_files("/data2/Sanjay/tiles/alg1"); 
    std::vector<tup_filesize> v2;
    for ( int i =0; i < v.size(); i++ ) {
        tup t = v[i];
        tile_result result = other_tile_exists(t.get<0>(), t.get<1>());
        if (result.exists) {
            path file1(result.files[0]);
            path file2(result.files[1]);
            int size = file_size(file1) + file_size(file2);
            tup_filesize t2(result.files[0], result.files[1], size);
            v2.push_back(t2);
        }
    }

    std::sort(v2.begin(), v2.end(), my_compare_op());
    std::reverse(v2.begin(), v2.end());
    int max_items = 0;
    cout.setf(ios::fixed);
    for ( int i =0; i < v2.size(); i++ ) {
        tup_filesize t = v2[i];
        int index = get_lightest_bin(bins);
        bins[index].items.push_back(t);
        bins[index].total_weight += t.get<2>();
        //std::cout << t.get<0>() << " " << t.get<1>() << std::endl;
        if (bins[index].items.size() > max_items) {
            max_items = bins[index].items.size();
        }

    }
    printBins(bins, max_items); 
    //int index = get_lightest_bin(bins);
    //std::cout << "index: " << index << std::endl;
    
}

/*
class Item {
    private:
        std::string filename;
        int size;
    public:
        Item();
        Item(std::string filename, int size);
        ~Item();
};

Item::Item() {
    this->filename = "";
    this->size = 0;
}
Item::Item(std::string filename, int size) {
    this->filename = filename;
    this->size = size;
}
Item::~Item() {

}
*/
