#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>

#include <errno.h>
#include <inttypes.h>
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
#include <boost/scoped_ptr.hpp>

#include <Lintel/HashUnique.hpp>
#include <Lintel/LintelLog.hpp>
#include <Lintel/PriorityQueue.hpp>
#include <Lintel/ProgramOptions.hpp>
#include <Lintel/STLUtility.hpp>

#include <DataSeries/DSExpr.hpp>
#include <DataSeries/GeneralField.hpp>
#include <DataSeries/RowAnalysisModule.hpp>
#include <DataSeries/SequenceModule.hpp>
#include <DataSeries/TFixedField.hpp>
#include <DataSeries/TypeIndexModule.hpp>

#include "GVVec.hpp"
#include "ServerModules.hpp"
#include "ThrowError.hpp"

using namespace std;
using namespace facebook::thrift;
using namespace facebook::thrift::protocol;
using namespace facebook::thrift::transport;
using namespace facebook::thrift::server;
using namespace dataseries;
using boost::shared_ptr;
using boost::format;
using boost::scoped_ptr;

namespace std {
ostream &operator <<(ostream &to, const vector<string> &v) {
    for(vector<string>::const_iterator i = v.begin(); i != v.end(); ++i) {
        if (i != v.begin()) {
            to << " ";
        }
        to << *i;
    }
    return to;
}
}

class DataSeriesServerHandler : public DataSeriesServerIf, public ThrowError {
public:
    struct TableInfo {
	const ExtentType *extent_type;
	vector<string> depends_on;
	Clock::Tfrac last_update;
	TableInfo() : extent_type(), last_update(0) { }
    };

    typedef HashMap<string, TableInfo> NameToInfo;

    DataSeriesServerHandler() { };

    void ping() {
	LintelLog::info("ping()");
    }

    void shutdown() {
	LintelLog::info("shutdown()");
        exit(0);
    }

    bool hasTable(const string &table_name) {
        try {
            getTableInfo(table_name);
            return true;
        } catch (TException &e) {
            return false;
        }
    }

    void importDataSeriesFiles(const vector<string> &source_paths, const string &extent_type, 
			       const string &dest_table) {
	verifyTableName(dest_table);
	if (extent_type.empty()) {
	    requestError("extent type empty");
	}

	TypeIndexModule input(extent_type);
	DataSeriesModule::Ptr output_module = makeTeeModule(input, tableToPath(dest_table));
	BOOST_FOREACH(const string &path, source_paths) {
	    input.addSource(path);
	}
	output_module->getAndDeleteShared();
	TableInfo &ti(table_info[dest_table]);
	ti.extent_type = input.getType();
	ti.last_update = Clock::todTfrac();
    }

    void importCSVFiles(const vector<string> &source_paths, const string &xml_desc, 
                        const string &dest_table, const string &field_separator,
                        const string &comment_prefix) {
	if (source_paths.empty()) {
	    requestError("missing source paths");
	}
        if (source_paths.size() > 1) {
            requestError("only supporting single insert");
        }
	verifyTableName(dest_table);
        pid_t pid = fork();
        if (pid < 0) {
            requestError("fork failed");
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

            LintelLogDebug("child", format("pid %d running: %s") % getpid() % args);
            exec(args);
        } else {
            waitForSuccessfulChild(pid);

            ExtentTypeLibrary lib;
            const ExtentType::Ptr type(lib.registerTypePtr(xml_desc));
            updateTableInfo(dest_table, type.get());
        }
    }

    void importSQLTable(const string &dsn, const string &src_table, const string &dest_table) {
        verifyTableName(dest_table);

        pid_t pid = fork();
        if (pid < 0) {
            requestError("fork failed");
        } else if (pid == 0) {
            for(int i = 3; i < 100; ++i) {
                close(i);
            }
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
            DataSeriesSource source(tableToPath(dest_table));
            const ExtentType *t = source.getLibrary().getTypeByName(src_table);

            updateTableInfo(dest_table, t); 
        }
    }
        
    void importData(const string &dest_table, const string &xml_desc, const TableData &data) {
        verifyTableName(dest_table);

        if (data.more_rows) {
            requestError("can not handle more rows");
        }
        ExtentTypeLibrary lib;
        const ExtentType::Ptr type(lib.registerTypePtr(xml_desc));

        ExtentSeries output_series(type);
        DataSeriesSink output_sink(tableToPath(dest_table), Extent::compress_lzf, 1);
        OutputModule output_module(output_sink, output_series, type, 96*1024);

        output_sink.writeExtentLibrary(lib);

        vector<boost::shared_ptr<GeneralField> > fields;
        for (uint32_t i = 0; i < type->getNFields(); ++i) {
            GeneralField::Ptr tmp(GeneralField::make(output_series, type->getFieldName(i)));
            fields.push_back(tmp);
        }

        BOOST_FOREACH(const vector<NullableString> &row, data.rows) {
            output_module.newRecord();
            if (row.size() != fields.size()) {
                requestError("incorrect number of fields");
            }
            for (uint32_t i = 0; i < row.size(); ++i) {
                if (row[i].__isset.v) {
                    fields[i]->set(row[i].v);
                } else {
                    fields[i]->setNull();
                }
            }
        }

        updateTableInfo(dest_table, type.get());
    }

    void mergeTables(const vector<string> &source_tables, const string &dest_table) {
	if (source_tables.empty()) {
	    requestError("missing source tables");
	}
	verifyTableName(dest_table);
	vector<string> input_paths;
	input_paths.reserve(source_tables.size());
	string source_extent_type;
	BOOST_FOREACH(const string &table, source_tables) {
	    if (table == dest_table) {
		invalidTableName(table, "duplicated with destination table");
	    }
	    TableInfo *ti = table_info.lookup(table);
	    if (ti == NULL) {
		invalidTableName(table, "table not present");
	    }
	    if (source_extent_type.empty()) {
		source_extent_type = ti->extent_type->getName();
	    }
	    if (source_extent_type != ti->extent_type->getName()) {
		invalidTableName(table, str(format("extent type '%s' does not match earlier table"
                                                   " types of '%s'")
                                            % ti->extent_type % source_extent_type));
	    }
				       
	    input_paths.push_back(tableToPath(table));
	}
	if (source_extent_type.empty()) {
	    requestError("internal: extent type is missing?");
	}
	importDataSeriesFiles(input_paths, source_extent_type, dest_table);
    }

    void getTableData(TableData &ret, const string &source_table, int32_t max_rows, 
                      const string &where_expr) {
        verifyTableName(source_table);
        if (max_rows <= 0) {
            requestError("max_rows must be > 0");
        }
        NameToInfo::iterator i = getTableInfo(source_table);

        TypeIndexModule input(i->second.extent_type->getName());
        input.addSource(tableToPath(source_table));
        DataSeriesModule *mod = &input;
        DataSeriesModule::Ptr select_module;
        if (!where_expr.empty()) {
            select_module = makeSelectModule(input, where_expr);
            mod = select_module.get();
        }

        DataSeriesModule::Ptr sink(makeTableDataModule(*mod, ret, max_rows));

        sink->getAndDeleteShared();
    }

    void hashJoin(const string &a_table, const string &b_table, const string &out_table,
                  const map<string, string> &eq_columns, 
                  const map<string, string> &keep_columns, int32_t max_a_rows) { 
        NameToInfo::iterator a_info = getTableInfo(a_table);
        NameToInfo::iterator b_info = getTableInfo(b_table);

        verifyTableName(out_table);

        TypeIndexModule a_input(a_info->second.extent_type->getName());
        a_input.addSource(tableToPath(a_table));
        TypeIndexModule b_input(b_info->second.extent_type->getName());
        b_input.addSource(tableToPath(b_table));

        OutputSeriesModule::OSMPtr 
            hj_module(makeHashJoinModule(a_input, max_a_rows, b_input,
                                         eq_columns, keep_columns, out_table));

        DataSeriesModule::Ptr output_module = makeTeeModule(*hj_module, tableToPath(out_table));
        
        output_module->getAndDeleteShared();
        updateTableInfo(out_table, hj_module->output_series.getType());
    }

    void starJoin(const string &fact_table, const vector<Dimension> &dimensions, 
                  const string &out_table, const map<string, string> &fact_columns,
                  const vector<DimensionFactJoin> &dimension_columns, int32_t max_dimension_rows) {
        NameToInfo::iterator fact_info = getTableInfo(fact_table);
        verifyTableName(out_table);
        
        HashMap< string, shared_ptr<DataSeriesModule> > dimension_modules;
        
        BOOST_FOREACH(const Dimension &dim, dimensions) {
            if (!dimension_modules.exists(dim.source_table)) {
                NameToInfo::iterator dim_info = getTableInfo(dim.source_table);
                shared_ptr<TypeIndexModule> 
                    ptr(new TypeIndexModule(dim_info->second.extent_type->getName()));
                ptr->addSource(tableToPath(dim.source_table));
                dimension_modules[dim.source_table] = ptr;
            }
        }
        
        TypeIndexModule fact_input(fact_info->second.extent_type->getName());
        fact_input.addSource(tableToPath(fact_table));

        // TODO: use and check max_dimension_rows
        OutputSeriesModule::OSMPtr
            sj_module(makeStarJoinModule(fact_input, dimensions, out_table,
                                         fact_columns, dimension_columns, dimension_modules));

        DataSeriesModule::Ptr output_module = makeTeeModule(*sj_module, tableToPath(out_table));

        output_module->getAndDeleteShared();
        updateTableInfo(out_table, sj_module->output_series.getType());
    }
    
    void selectRows(const string &in_table, const string &out_table, const string &where_expr) {
        verifyTableName(in_table);
        verifyTableName(out_table);
        NameToInfo::iterator info = getTableInfo(in_table);
        TypeIndexModule input(info->second.extent_type->getName());
        input.addSource(tableToPath(in_table));
        DataSeriesModule::Ptr select(makeSelectModule(input, where_expr));
        DataSeriesModule::Ptr output_module = makeTeeModule(*select, tableToPath(out_table));

        output_module->getAndDeleteShared();
        updateTableInfo(out_table, info->second.extent_type);
    }

    void projectTable(const string &in_table, const string &out_table, 
                      const vector<string> &keep_columns) {
        verifyTableName(in_table);
        verifyTableName(out_table);

        NameToInfo::iterator info = getTableInfo(in_table);
        TypeIndexModule input(info->second.extent_type->getName());
        input.addSource(tableToPath(in_table));
        OutputSeriesModule::OSMPtr project(makeProjectModule(input, keep_columns));
        DataSeriesModule::Ptr output_module = makeTeeModule(*project, tableToPath(out_table));
        output_module->getAndDeleteShared();
        updateTableInfo(out_table, project->output_series.getType());
    }

    void transformTable(const string &in_table, const string &out_table,
                        const vector<ExprColumn> &expr_columns) {
        verifyTableName(in_table);
        verifyTableName(out_table);

        NameToInfo::iterator info = getTableInfo(in_table);
        TypeIndexModule input(info->second.extent_type->getName());
        input.addSource(tableToPath(in_table));
        OutputSeriesModule::OSMPtr transform
            (makeExprTransformModule(input, expr_columns, out_table));
        DataSeriesModule::Ptr output_module = makeTeeModule(*transform, tableToPath(out_table));
        output_module->getAndDeleteShared();
        updateTableInfo(out_table, transform->output_series.getType());
    }
    
    void sortedUpdateTable(const string &base_table, const string &update_from, 
                           const string &update_column, const vector<string> &primary_key) {
        verifyTableName(base_table);
        verifyTableName(update_from);

        NameToInfo::iterator update_info(getTableInfo(update_from));
        NameToInfo::iterator base_info = table_info.find(base_table);
        if (base_info == table_info.end()) {
            base_info = createTable(base_table, update_info, update_column);
        }

        TypeIndexModule base_input(base_info->second.extent_type->getName());
        base_input.addSource(tableToPath(base_table));

        TypeIndexModule update_input(update_info->second.extent_type->getName());
        update_input.addSource(tableToPath(update_from));

        DataSeriesModule::Ptr updater(makeSortedUpdateModule(base_input, update_input, 
                                                             update_column, primary_key));

        DataSeriesModule::Ptr output_module 
            = makeTeeModule(*updater, tableToPath(base_table, "tmp."));
        output_module->getAndDeleteShared();
        output_module.reset();
        string from(tableToPath(base_table, "tmp.")), to(tableToPath(base_table));
        int ret = rename(from.c_str(), to.c_str());
        INVARIANT(ret == 0, format("rename %s -> %s failed: %s") % from % to % strerror(errno));
    }

    void unionTables(const vector<UnionTable> &in_tables, const vector<SortColumn> &order_columns,
                     const string &out_table) {
        vector<UM_UnionTable> tables;
        BOOST_FOREACH(const UnionTable &table, in_tables) {
            verifyTableName(table.table_name);
            NameToInfo::iterator table_info(getTableInfo(table.table_name));

            TypeIndexModule::Ptr p =
                TypeIndexModule::make(table_info->second.extent_type->getName());
            p->addSource(tableToPath(table_info->first));
            tables.push_back(UM_UnionTable(table, p));
        }
        
        OutputSeriesModule::OSMPtr union_mod(makeUnionModule(tables, order_columns, out_table));
        DataSeriesModule::Ptr output_module = makeTeeModule(*union_mod, tableToPath(out_table));

        output_module->getAndDeleteShared();
        updateTableInfo(out_table, union_mod->output_series.getType());
    }

    void sortTable(const string &in_table, const string &out_table, const vector<SortColumn> &by) {
        NameToInfo::iterator table_info(getTableInfo(in_table));
        
        TypeIndexModule::Ptr p(TypeIndexModule::make(table_info->second.extent_type->getName()));
        p->addSource(tableToPath(table_info->first));

        OutputSeriesModule::OSMPtr sorter(makeSortModule(*p, by));
        
        DataSeriesModule::Ptr output_module = makeTeeModule(*sorter, tableToPath(out_table));
        output_module->getAndDeleteShared();
        updateTableInfo(out_table, sorter->output_series.getType());
    }

private:
    void verifyTableName(const string &name) {
	if (name.size() >= 200) {
	    invalidTableName(name, "name too long");
	}
	if (name.find('/') != string::npos) {
	    invalidTableName(name, "contains /");
	}
    }

    string tableToPath(const string &table_name, const string &prefix = "ds.") {
        verifyTableName(table_name);
        return prefix + table_name;
    }

    void updateTableInfo(const string &table, const ExtentType *extent_type) {
        TableInfo &info(table_info[table]);
        info.extent_type = extent_type;
    }

    void waitForSuccessfulChild(pid_t pid) {
        int status = -1;
        if (waitpid(pid, &status, 0) != pid) {
            requestError("waitpid() failed");
        }
        LintelLogDebug("child", format("child %d returned %d") % pid % status);
        if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
            requestError("csv2ds failed");
        }
    }

    NameToInfo::iterator 
    createTable(const string &table_name, NameToInfo::iterator update_table, 
                const std::string &update_column) {
        const ExtentType *from_type = update_table->second.extent_type;
        string extent_type = str(format("<ExtentType name=\"%s\" namespace=\"%s\""
                                        " version=\"%d.%d\">") % table_name 
                                 % from_type->getNamespace() % from_type->majorVersion()
                                 % from_type->minorVersion());

        for (uint32_t i = 0; i < from_type->getNFields(); ++i) {
            string field_name = from_type->getFieldName(i);
            if (field_name == update_column) {
                continue; // ignore
            }
            extent_type.append(from_type->xmlFieldDesc(field_name));
        }
        extent_type.append("</ExtentType>");

        DataSeriesSink output(tableToPath(table_name), Extent::compress_lzf, 1);
        ExtentTypeLibrary library;
        const ExtentType::Ptr type(library.registerTypePtr(extent_type));
        output.writeExtentLibrary(library);
        Extent tmp(type);
        output.writeExtent(tmp, NULL);
        output.close();
        updateTableInfo(table_name, library.getTypeByName(table_name));
        return getTableInfo(table_name);
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

    NameToInfo::iterator getTableInfo(const string &table_name) {
        NameToInfo::iterator ret = table_info.find(table_name);
        if (ret == table_info.end()) {
            invalidTableName(table_name, "table missing");
        }
        return ret;
    }

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
    LintelLog::parseEnv();
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
