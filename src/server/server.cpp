#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>

#include <errno.h>
#include <pwd.h>
#include <unistd.h>

#include <concurrency/ThreadManager.h>
#include <concurrency/PosixThreadFactory.h>
#include <protocol/TBinaryProtocol.h>
#include <server/TSimpleServer.h>
#include <transport/TServerSocket.h>
#include <transport/TTransportUtils.h>

#include <boost/foreach.hpp>
#include <boost/shared_ptr.hpp>

#include <Lintel/LintelLog.hpp>
#include <Lintel/ProgramOptions.hpp>

#include <DataSeries/RowAnalysisModule.hpp>
#include <DataSeries/GeneralField.hpp>
#include <DataSeries/TypeIndexModule.hpp>

#include "gen-cpp/DataSeriesServer.h"

using namespace std;
using namespace facebook::thrift;
using namespace facebook::thrift::protocol;
using namespace facebook::thrift::transport;
using namespace facebook::thrift::server;
using namespace dataseries;
using boost::shared_ptr;
using boost::format;

class TeeModule : public RowAnalysisModule {
public:
    TeeModule(DataSeriesModule &source_module, const string &output_path)
	: RowAnalysisModule(source_module), output_path(output_path), 
	  output_series(), output(output_path, Extent::compress_lzf, 1),
	  output_module(NULL), copier(series, output_series), row_count(0)
    { }

    virtual ~TeeModule() {
	delete output_module;
    }

    virtual void firstExtent(const Extent &e) {
	// series.setType(e.getType());
	output_series.setType(e.getType());

	output_module = new OutputModule(output, output_series, e.getType(), 96*1024);
	copier.prep();
	ExtentTypeLibrary library;
	library.registerType(e.getType());
	output.writeExtentLibrary(library);
    }

    virtual void processRow() {
	++row_count;
	output_module->newRecord();
	copier.copyRecord();
    }

    const string output_path;
    ExtentSeries output_series;
    DataSeriesSink output;
    OutputModule *output_module;
    ExtentRecordCopy copier;
    uint64_t row_count;
};

class TableDataModule : public RowAnalysisModule {
public:
    TableDataModule(DataSeriesModule &source_module, TableData &into, uint32_t max_rows)
        : RowAnalysisModule(source_module), into(into), max_rows(max_rows)
    { 
        into.rows.reserve(max_rows < 4096 ? max_rows : 4096);
        into.more_rows = false;
    }

    ~TableDataModule() {
        BOOST_FOREACH(GeneralField *g, fields) {
            delete g;
        }
    }

    virtual void firstExtent(const Extent &e) {
        series.setType(e.getType());
        const ExtentType &extent_type(e.getType());
        fields.reserve(extent_type.getNFields());
        for (uint32_t i = 0; i < extent_type.getNFields(); ++i) {
            fields.push_back(GeneralField::create(series, extent_type.getFieldName(i)));
        }
    }

    virtual void processRow() {
        if (into.rows.size() == max_rows) {
            into.more_rows = true;
            return;
        }
        into.rows.resize(into.rows.size() + 1);
        vector<string> &row(into.rows.back());
        row.reserve(fields.size());
        BOOST_FOREACH(GeneralField *g, fields) {
            row.push_back(g->val().valString());
        }
    }

    TableData &into;
    uint32_t max_rows;
    vector<GeneralField *> fields;
};

class DataSeriesServerHandler : public DataSeriesServerIf {
public:
    DataSeriesServerHandler() { };

    void ping() {
	LintelLog::info("ping()");
    }

    void verifyTableName(const string &name) {
	if (name.size() >= 200) {
	    throw InvalidTableName(name, "name too long");
	}
	if (name.find('/') != string::npos) {
	    throw InvalidTableName(name, "contains /");
	}
    }

    void importDataSeriesFiles(const vector<string> &source_paths, const string &extent_type, 
			       const string &dest_table) {
	verifyTableName(dest_table);
	if (extent_type.empty()) {
	    throw RequestError("extent type empty");
	}

	TypeIndexModule input(extent_type);
	TeeModule tee_op(input, tableToPath(dest_table));
	BOOST_FOREACH(const string &path, source_paths) {
	    input.addSource(path);
	}
	tee_op.getAndDelete();
	TableInfo &ti(table_info[dest_table]);
	ti.extent_type = extent_type;
	ti.last_update = Clock::todTfrac();
    }

    void importCSVFiles(const vector<string> &source_paths, const string &xml_desc, 
                        const string &dest_table, const string &field_separator,
                        const string &comment_prefix) {
	if (source_paths.empty()) {
	    throw RequestError("missing source paths");
	}
        if (source_paths.size() > 1) {
            throw RequestError("only supporting single insert");
        }
	verifyTableName(dest_table);
        pid_t pid = fork();
        if (pid < 0) {
            throw RequestError("fork failed");
        } else if (pid == 0) {
            string xml_desc_path(str(format("xmldesc.%s") % dest_table));
            ofstream xml_desc_output(xml_desc_path.c_str());
            xml_desc_output << xml_desc;
            xml_desc_output.close();
            SINVARIANT(xml_desc_output.good());

            vector<string> args;
            args.push_back("csv2ds");
            args.push_back(str(format("--xml-desc-file=%s") % xml_desc_path));
            args.push_back(str(format("--field-separator=%s") % field_separator));
            args.push_back(str(format("--comment-prefix=%s") % comment_prefix));
            SINVARIANT(source_paths.size() == 1);
            copy(source_paths.begin(), source_paths.end(), back_inserter(args));
            args.push_back(tableToPath(dest_table));
            unlink(tableToPath(dest_table).c_str()); // ignore errors

            exec(args);
        } else {
            waitForSuccessfulChild(pid);

            ExtentTypeLibrary lib;
            const ExtentType &type(lib.registerTypeR(xml_desc));
            updateTableInfo(dest_table, type.getName());
        }
    }

    void importSQLTable(const string &dsn, const string &src_table, const string &dest_table) {
        verifyTableName(dest_table);

        pid_t pid = fork();
        if (pid < 0) {
            throw RequestError("fork failed");
        } else if (pid == 0) {
            vector<string> args;

            args.push_back("sql2ds");
            if (!dsn.empty()) {
                args.push_back(str(format("--dsn=%s") % dsn));
            }
            args.push_back(src_table);
            args.push_back(tableToPath(dest_table));
            exec(args);
        } else {
            waitForSuccessfulChild(pid);
            updateTableInfo(dest_table, src_table); // sql2ds extent type name = src table
        }
    }
        

    void mergeTables(const vector<string> &source_tables, const string &dest_table) {
	if (source_tables.empty()) {
	    throw RequestError("missing source tables");
	}
	verifyTableName(dest_table);
	vector<string> input_paths;
	input_paths.reserve(source_tables.size());
	string source_extent_type;
	BOOST_FOREACH(const string &table, source_tables) {
	    if (table == dest_table) {
		throw InvalidTableName(table, "duplicated with destination table");
	    }
	    TableInfo *ti = table_info.lookup(table);
	    if (ti == NULL) {
		throw InvalidTableName(table, "table not present");
	    }
	    if (source_extent_type.empty()) {
		source_extent_type = ti->extent_type;
	    }
	    if (source_extent_type != ti->extent_type) {
		throw InvalidTableName(table, str(format("extent type '%s' does not match earlier table types of '%s'")
						  % ti->extent_type % source_extent_type));
	    }
				       
	    input_paths.push_back(tableToPath(table));
	}
	if (source_extent_type.empty()) {
	    throw RequestError("internal: extent type is missing?");
	}
	importDataSeriesFiles(input_paths, source_extent_type, dest_table);
    }

    void getTableData(TableData &ret, const string &source_table, int32_t max_rows) {
        verifyTableName(source_table);
        if (max_rows <= 0) {
            throw RequestError("max_rows must be > 0");
        }
        NameToInfo::iterator i = table_info.find(source_table);
        if (i == table_info.end()) {
            throw TApplicationException("table missing");
        }

        TypeIndexModule input(i->second.extent_type);
        input.addSource(tableToPath(source_table));
        TableDataModule sink(input, ret, max_rows);

        sink.getAndDelete();
    }

    void test() {
	TypeIndexModule input("NFS trace: attr-ops");

	input.addSource("/home/anderse/projects/DataSeries/check-data/nfs.set6.20k.ds");
	TeeModule tee_op(input, "ds.test");
	tee_op.getAndDelete();
    }

private:
    string tableToPath(const string &table_name) {
        return string("ds.") + table_name;
    }

    void updateTableInfo(const string &table, const string &extent_type) {
        TableInfo &info(table_info[table]);
        info.extent_type = extent_type;
    }

    void waitForSuccessfulChild(pid_t pid) {
        int status = -1;
        if (waitpid(pid, &status, 0) != pid) {
            throw RequestError("waitpid() failed");
        }
        if (WEXITSTATUS(status) != 0) {
            throw RequestError("csv2ds failed");
        }
    }

    void exec(vector<string> &args) {
        char **argv = new char *[args.size() + 1];
        const char *tmp;
        for (uint32_t i = 0; i < args.size(); ++i) {
            tmp = args[i].c_str(); // force null termination
            argv[i] = &args[i][0]; // couldn't figure out how to directly use c_str()
        }
        argv[args.size()] = NULL;
        execvp(args[0].c_str(), argv);
        FATAL_ERROR(format("exec of %s failed: %s") % args[0] % strerror(errno));
    }

    struct TableInfo {
	string extent_type;
	vector<string> depends_on;
	Clock::Tfrac last_update;
	TableInfo() : extent_type(), last_update(0) { }
    };

    typedef HashMap<string, TableInfo> NameToInfo;
    NameToInfo table_info;
};

lintel::ProgramOption<string> po_working_directory
("working-directory", "Specifies the working directory for cached intermediate tables");

void setupWorkingDirectory() {
    string working_directory = po_working_directory.get();
    if (!po_working_directory.used()) {
	working_directory = "/tmp/ds-server.";
	struct passwd *p = getpwuid(getuid());
	SINVARIANT(p != NULL);
	working_directory += p->pw_name;
    }

    struct stat stat_buf;
    int ret = stat(working_directory.c_str(), &stat_buf);
    CHECKED((ret == -1 && errno == ENOENT) || (ret == 0 && S_ISDIR(stat_buf.st_mode)),
	    format("Error accessing %s: %s") % working_directory
	    % (ret == 0 ? "not a directory" : strerror(errno)));
    if (ret == -1 && errno == ENOENT) {
	CHECKED(mkdir(working_directory.c_str(), 0777) == 0,
		format("Unable to create directory %s: %s") % working_directory % strerror(errno));
    }
    CHECKED(chdir(working_directory.c_str()) == 0,
	    format("Unable to chdir to %s: %s") % working_directory % strerror(errno));
}

int main(int argc, char *argv[]) {
    shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
    shared_ptr<DataSeriesServerHandler> handler(new DataSeriesServerHandler());
    shared_ptr<TProcessor> processor(new DataSeriesServerProcessor(handler));
    shared_ptr<TServerTransport> serverTransport(new TServerSocket(49476));
    shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());

    TSimpleServer server(processor, serverTransport, transportFactory, protocolFactory);

    setupWorkingDirectory();

    LintelLog::info("start...");
    server.serve();
    LintelLog::info("finish.");

    return 0;
}
