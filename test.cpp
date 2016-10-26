#include <string>
#include <vector>
#include <iostream>
#include <map>

using namespace std;

int main(int argc, char* argv[]) 
{
    map<string,string> hostmap;
    hostmap["astroII.1"] = "node32.clus.cci.emory.edu";
    hostmap["astroII.2"] = "node32.clus.cci.emory.edu";

    hostmap["gbm0.1"] = "node33.clus.cci.emory.edu";
    hostmap["gbm0.2"] = "node33.clus.cci.emory.edu";

    hostmap["gbm1.1"] = "node34.clus.cci.emory.edu";
    hostmap["gbm1.2"] = "node34.clus.cci.emory.edu";

    hostmap["gbm2.1"] = "node35.clus.cci.emory.edu";
    hostmap["gbm2.2"] = "node35.clus.cci.emory.edu";

    hostmap["normal.2"] = "node36.clus.cci.emory.edu";
    hostmap["normal.3"] = "node36.clus.cci.emory.edu";

    hostmap["oligoastroII.1"] = "node37.clus.cci.emory.edu";
    hostmap["oligoastroII.2"] = "node37.clus.cci.emory.edu";

    hostmap["oligoastroIII.1"] = "node38.clus.cci.emory.edu";
    hostmap["oligoastroIII.2"] = "node38.clus.cci.emory.edu";

    hostmap["oligoII.1"] = "node39.clus.cci.emory.edu";
    hostmap["oligoII.2"] = "node39.clus.cci.emory.edu";

    hostmap["oligoIII.1"] = "node40.clus.cci.emory.edu";
    hostmap["oligoIII.2"] = "node40.clus.cci.emory.edu";

    cout << hostmap["oligoIII.1"] << endl;
    string key = "oligoIII.1";
    string s = "test";
    if (s.compare(hostmap[key]) == 0 ) 
        cout << "same" << endl;
    else
        cout << "not same" << endl;
    return 0;
}

