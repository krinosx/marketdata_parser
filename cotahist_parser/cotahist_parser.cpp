// cotahist_parser.cpp : Defines the entry point for the application.
//

#include "cotahist_parser.h"
using namespace std;


#ifdef __unix__
 #define LINE_SIZE 246
#elif _WIN32 || _WIN64 
 #define LINE_SIZE 247
#endif


string inputFileName;
string databaseFileName;

struct STOCK_DATA {
    string data_pregao; // 03-10 (08)
    string codigo_negociacao; // 11-12 (02)
    string tipo_mercado; // 25-27 - (03)
    string nome_resumido; // 28-39 (12)
    string moeda; // 53-56 (04)
    double preco_abertura = 0.0; // 57-69 - (11)
    double preco_maximo = 0.0; // 70-82 - (11)
    double preco_minimo = 0.0; // 83-95 - (11)
    double preco_medio = 0.0; // 96-108 - (11)
    double preco_fechamento = 0.0; //109-121 (11)

    int total_negocios = 0; // 148-152 (5)
    int qtd_titulos_negociados = 0; // 153-170 (18)
    double volume_financeiro_negociado = 0.0; // 171-188 (16)

    int fator_cotacao = 0; // 211-217 (07)
    string codigo_isin; // 231-242 (12)
};


int parseData(string *line, STOCK_DATA * data)
{

    string dataDoPregao = line->substr(2, 8);
    
    dataDoPregao = dataDoPregao.insert(4, "-");
    dataDoPregao = dataDoPregao.insert(7, "-");

    data->data_pregao = dataDoPregao;

    data->codigo_negociacao = line->substr(12, 12);
    data->codigo_isin = line->substr(230, 12);
    
    data->tipo_mercado = line->substr(24, 3);
    data->nome_resumido = line->substr(27, 12);
    data->moeda = line->substr(52, 4);

    string rawNumber = line->substr(56, 13);
    data->preco_abertura = stod(rawNumber.insert(rawNumber.length() - 2, ".") );

    rawNumber = line->substr(108, 13);
    data->preco_fechamento = stod( rawNumber.insert(rawNumber.length() - 2, ".") );

    rawNumber = line->substr(69, 13);
    data->preco_maximo = stod(rawNumber.insert(rawNumber.length() - 2, "."));

    rawNumber = line->substr(82, 13);
    data->preco_minimo = stod(rawNumber.insert(rawNumber.length() - 2, "."));

    rawNumber = line->substr(95, 13);
    data->preco_medio = stod(rawNumber.insert(rawNumber.length() - 2, "."));

    rawNumber = line->substr(147, 5);
    data->total_negocios = stoi(rawNumber);

    rawNumber = line->substr(152, 18);
    data->qtd_titulos_negociados = stod(rawNumber);

    rawNumber = line->substr(170, 18);
    data->volume_financeiro_negociado = stod(rawNumber.insert(rawNumber.length() - 2, "."));

    rawNumber = line->substr(210, 7);
    data->fator_cotacao = stoi(rawNumber);

    return SQLITE_OK;
}



int createTables(sqlite3 *database) {

    string createMarketDataQuery = 
        "CREATE TABLE IF NOT EXISTS MKT_COTACAO ( "
        " ISINCODE TEXT NOT NULL, "
        " CODIGO_NEGOCIACAO TEXT NOT NULL, "
        " DATA_PREGAO INTEGER NOT NULL, "
        " MOEDA TEXT, "
        " PRECO_ABERTURA REAL, " 
        " PRECO_FECHAMENTO REAL, "
        " PRECO_MAXIMO REAL, "
        " PRECO_MINIMO REAL, "
        " PRECO_MEDIO REAL, "
        " TOTAL_NEGOCIOS INTEGER, "
        " QTD_TITULOS_NEGOCIADOS INTEGER, "
        " VOLUME_FINANCEIRO_NEGOCIADO INTEGER, "
        " FATOR_COTACAO INTEGER, "
        " PRIMARY KEY (ISINCODE,DATA_PREGAO)"
        " )";

    char *errorMessage;

    int returnCode = sqlite3_exec(database, createMarketDataQuery.c_str(), NULL, NULL, &errorMessage );

    if (returnCode != SQLITE_OK)
    {
        cout << "Unable to create database table MKT_COTACAO!! ErrorMessage [ " << errorMessage << " ] " <<  endl;
        
        sqlite3_free(errorMessage);

        return SQLITE_ERROR;
    }

    return SQLITE_OK;
}
void printUsage() {
    cout << " Usage: cotahist_parser -f <InputFileName> -d <DatabaseFileName> " << endl;
    cout << endl;
    cout << " -f full path to input file " << endl;
    cout << " -d full path to database file. If file does not exist it will be created. " << endl;
    cout << endl;
}
// CHECK PARAMETERS
// -d databasename
// -f inputfilename
bool checkUsage(int argc, char* argv[]) {
    int error = 0;
    if (argc < 5)
    {
        printUsage();
        return false;
    }

    string argvline = *argv;
    size_t dPos = argvline.find("-d");
    size_t fPos = argvline.find("-f");

    if (dPos < 0 || fPos < 0) {
        printUsage();
        return false;
    }

    for (int i = 1; i < argc; i++)
    {
        if (string(argv[i]) == "-f") {
            inputFileName = argv[++i];
        }
        if (string(argv[i]) == "-d") {
            databaseFileName = argv[++i];
        }
    }

    bool paramError = false;
    if (inputFileName.empty()) {
        printUsage();
        cout << endl << " --> InputFile name is empty. Provide a valid input file name." << endl;
        paramError = true;
    }
    if (databaseFileName.empty()) {
        printUsage();
        cout << endl << " --> Database file name is empty. Provide a valid database file name." << endl;
        paramError = true;
    }
    return !paramError;
    
}

int main(int argc, char* argv[])
{
    if (!checkUsage(argc, argv)) {
        exit(-1);
    }

    cout << " Input file:    [" << inputFileName << "]" << endl;
    cout << " Database file: [" << databaseFileName << "]" << endl;

    cout << " Checking Database" << endl << endl;
    sqlite3 *database;
    int errorCode = sqlite3_open_v2( databaseFileName.c_str(), &database, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);

    if (errorCode)
    {
        cout << " Cant open database! Exiting. Error: " << sqlite3_errmsg(database) << endl;
        exit(-1);
    }
    
    int databaseStatus = createTables(database);
    if (databaseStatus != SQLITE_OK)
    {
        cout << " Cant open table MKT_COTACAO! Exiting. Error: " << sqlite3_errmsg(database) << endl;
        exit(-1);
    }

    cout << " Database ok!" << endl << endl;

    cout << " Parsing input file " << inputFileName << endl;

    //string filename;
    //filename = "E:/Projetos/marketdata_parser/cotahist_parser/cotahist_parser/data/COTAHIST_A2018.TXT";

    string line;

    ifstream fileHandler;
    fileHandler.open(inputFileName, ifstream::binary);
    

    int numLines = 0;

    if (fileHandler.is_open()) {
        // parse the header
        string header;
        getline(fileHandler, header);

        string dataArquivo = header.substr(23, 8);
        cout << " Data date: " << dataArquivo <<  endl;


        // parse the trail
        // TODO: Check a way to make it cross platform. In windows we use
        // 245 position of data on each line + 2 for endln characters (\r\n)
        // on unix we use 245 data positions + 1 for endl (\n)
        string trail;
        fileHandler.seekg(-LINE_SIZE, ios_base::end);
        getline(fileHandler, trail);
        
        string registers;
        int registersCount = 0;

        registers = trail.substr(31, 11);

        try {
            registersCount = stoi(registers);
        }
        catch (const invalid_argument& ia) {
            cerr << " Invalid register amount found in trail. Trail data:[ " << registers << " ]. " << ia.what() << endl;
            exit(-1);
        }
        cout << " Total Registers: " << registersCount << endl;


        // Move pointer to start
        fileHandler.seekg(0, ios_base::beg);


        registersCount = registersCount - 2; // discount header and trailer

        string insertQuery = 
            "INSERT INTO MKT_COTACAO ( "
            " ISINCODE, "
            " CODIGO_NEGOCIACAO, "
            " DATA_PREGAO, "
            " MOEDA, "
            " PRECO_ABERTURA, "
            " PRECO_FECHAMENTO, "
            " PRECO_MAXIMO, "
            " PRECO_MINIMO, "
            " PRECO_MEDIO, "
            " TOTAL_NEGOCIOS, "
            " QTD_TITULOS_NEGOCIADOS, "
            " VOLUME_FINANCEIRO_NEGOCIADO, "
            " FATOR_COTACAO ) VALUES ( ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13 ) ON CONFLICT DO NOTHING;";
        
            int completeCode = sqlite3_complete(insertQuery.c_str());

        if (completeCode != 1) {
            cout << " Incomplete. Error Code " << completeCode << "ErrorMessage: " << sqlite3_errmsg(database) << endl;
        }

        sqlite3_stmt *insertStmt;
        int prepareResult = sqlite3_prepare_v2(database, insertQuery.c_str(), -1, &insertStmt, 0);

        if (prepareResult != SQLITE_OK) {
            cout << " Error to prepare statement: " << prepareResult << " Mesage: " << sqlite3_errmsg(database) << endl;
        }

        int rowsInserted = 0;
        int rowswithError = 0;
        int rowsIgnored = 0;
        string curentLine;
        clock_t begin = clock();
       
        char * error_begin_transaction;
        int rerturCodeBeginTransaction = sqlite3_exec(database, "BEGIN TRANSACTION;", NULL, NULL, &error_begin_transaction);
        
        if (rerturCodeBeginTransaction != SQLITE_OK) {
            cout << " Error to start transaction. " << sqlite3_errmsg(database) << endl;
        }
        // get and ignore first line (header)
        getline(fileHandler, header);

        for (int i = 0; i < registersCount; i++) {
           
            getline(fileHandler, curentLine);

            STOCK_DATA lineData;

            if (stoi(curentLine.substr(24, 3)) == 10) {
                if (parseData(&curentLine, &lineData) != SQLITE_OK) {
                    // Error
                    continue;
                }

                sqlite3_reset(insertStmt);

                sqlite3_bind_text(insertStmt, 1, lineData.codigo_isin.c_str(), -1, NULL);
                sqlite3_bind_text(insertStmt, 2, lineData.codigo_negociacao.c_str(), -1, NULL);
                sqlite3_bind_text(insertStmt, 3, lineData.data_pregao.c_str(), -1, NULL);
                sqlite3_bind_text(insertStmt, 4, lineData.moeda.c_str(), -1, NULL);
                sqlite3_bind_double(insertStmt, 5, lineData.preco_abertura);
                sqlite3_bind_double(insertStmt, 6, lineData.preco_fechamento);
                sqlite3_bind_double(insertStmt, 7, lineData.preco_maximo);
                sqlite3_bind_double(insertStmt, 8, lineData.preco_minimo);
                sqlite3_bind_double(insertStmt, 9, lineData.preco_medio);
                sqlite3_bind_int(insertStmt, 10, lineData.total_negocios);
                sqlite3_bind_int(insertStmt, 11, lineData.qtd_titulos_negociados);
                sqlite3_bind_double(insertStmt, 12, lineData.volume_financeiro_negociado);
                sqlite3_bind_int(insertStmt, 13, lineData.fator_cotacao);


                int insertResult = sqlite3_step(insertStmt);

                if (insertResult == SQLITE_DONE) {
                    rowsInserted++;
                    if (rowsInserted % 10000 == 0) {
                        cout << " " << rowsInserted << " inserted lines" << endl;
                    }
                }
                else { 
                    rowswithError++;
                    cout << " Error to insert line " << i << sqlite3_errmsg(database) << endl;
                }
            }
            else {
                rowsIgnored++;
            }
        }        

        char * error_end_transaction;
        int rerturCodeEndTransaction = sqlite3_exec(database, "END TRANSACTION;", NULL, NULL, &error_end_transaction);

        if (rerturCodeEndTransaction != SQLITE_OK) {
            cout << " Error to end transaction. " << sqlite3_errmsg(database) << "Error: " << error_end_transaction << endl;
        }

        fileHandler.close();
        cout << " Input file " << inputFileName << " closed. " << endl;
        // Finalize Insert STMT
        errorCode = sqlite3_finalize(insertStmt);
        

        if (errorCode != SQLITE_OK)
        {
            cout << " Cant close statement! Check for corrupted file!! Exiting" << endl;
            exit(-1);
        }

        errorCode = sqlite3_close_v2(database);
        if (errorCode != SQLITE_OK)
        {
            cout << " Cant close database! Check for corrupted file!! Exiting" << endl;
            exit(-1);
        }
        cout << " Database closed. " << endl;

        clock_t end = clock();
        double elapsed_secs = double(end - begin) / CLOCKS_PER_SEC;

        cout << " IMPORT FINISHED IN  " << elapsed_secs << " seconds. "<< endl;
        cout << " ------------------ SUMMARY ------------------ " << endl;
        cout << " Inserted: " << rowsInserted << endl;
        cout << " Error(s): " << rowswithError << endl;
        cout << " Ignored: " << rowsIgnored << endl;
        cout << " ------------------ SUMMARY ------------------ " << endl;

    }
    else {

         cout << "Failed to open file" <<  endl;
         return -1;
    }

     cout <<  endl << " Import process finished " <<  endl;

    return 0;

}
