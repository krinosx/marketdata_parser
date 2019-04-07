#include <iostream>
#include <string>
#include <fstream>
#include <iostream>

int main(){

 std::cout << " PARSING COTAHIST FILES " << std::endl;

 std::string filename;

 filename = "pregao_full.txt";

 std::string line;

 std::ifstream fileHandler;
 fileHandler.open(filename);


 int numLines = 0;

 if( fileHandler.is_open()) {


   // parse the header

   std::string header;
   getline(fileHandler, header);

   std::string dataArquivo = header.substr(23,8);

   std::cout << " DataArquivo: " << dataArquivo << std::endl;


   // for(int i=0; getline(fileHandler, line); ++numLines){

   //     std::cout << line << std::endl;

   // }

 fileHandler.close();

} else {
 
  std::cout << "Failed to open file" << std::endl;

}


std::cout << std::endl << " FINISHED PARSING COTAHIST FILES " << std::endl;

 return 0;


}
