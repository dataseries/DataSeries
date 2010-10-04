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

#include <Lintel/HashUnique.hpp>
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
	series.setType(e.getType());
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

struct GVVec {
    vector<GeneralValue> vec;

    bool operator ==(const GVVec &rhs) const {
        if (vec.size() != rhs.vec.size()) {
            return false;
        }
        vector<GeneralValue>::const_iterator i = vec.begin(), j = rhs.vec.begin();
        for (; i != vec.end(); ++i, ++j) {
            if (*i != *j) { 
                return false;
            }
        }
            
        return true;
    }

    uint32_t hash() const {
        uint32_t partial_hash = 1942;

        BOOST_FOREACH(const GeneralValue &gv, vec) {
            partial_hash = gv.hash(partial_hash);
        }
        return partial_hash;
    }

    void print (ostream &to) const {
        BOOST_FOREACH(const GeneralValue &gv, vec) {
            to << gv;
        }
    }

    void resize(size_t size) {
        vec.resize(size);
    }

    size_t size() const {
        return vec.size();
    }

    GeneralValue &operator [](size_t offset) {
        return vec[offset];
    }
};

inline ostream & operator << (ostream &to, GVVec &gvvec) {
    gvvec.print(to);
    return to;
}

class HashJoinModule : public DataSeriesModule {
public:
    typedef map<string, string> CMap; // column map

    HashJoinModule(DataSeriesModule &a_input, int32_t max_a_rows, DataSeriesModule &b_input,
                   const map<string, string> &eq_columns, const map<string, string> &keep_columns,
                   const string &output_table_name) 
        : a_input(a_input), b_input(b_input), max_a_rows(max_a_rows), 
          eq_columns(eq_columns), keep_columns(keep_columns), output_extent(NULL),
          output_table_name(output_table_name)
    { }

    class Extractor {
    public:
        typedef boost::shared_ptr<Extractor> Ptr;

        Extractor(const string &field_name) : field_name(field_name), into() { }
        virtual void extract(const GVVec &a_val) = 0;

        const string field_name;
        GeneralField::Ptr into;
    };

    class ExtractorField : public Extractor {
    public:
        static Ptr make(const string &field_name, GeneralField::Ptr from) {
            return Ptr(new ExtractorField(field_name, from));
        }
        virtual void extract(const GVVec &a_val) {
            into->set(from);
        }

    private:
        GeneralField::Ptr from;

        ExtractorField(const string &field_name, GeneralField::Ptr from) 
            : Extractor(field_name), from(from) 
        { }
    };

    class ExtractorValue : public Extractor {
    public:
        static Ptr make(const string &field_name, uint32_t pos) {
            return Ptr(new ExtractorValue(field_name, pos));
        }
        
        virtual void extract(const GVVec &a_val) {
            SINVARIANT(pos < a_val.size());
            into->set(a_val.vec[pos]);
        }

    private:
        uint32_t pos;

        ExtractorValue(const string &field_name, uint32_t pos) 
            : Extractor(field_name), pos(pos)
        { }
    };

    static const string mapDGet(const map<string, string> &a_map, const string &a_key) {
        map<string, string>::const_iterator i = a_map.find(a_key);
        SINVARIANT(i != a_map.end());
        return i->second;
    }

    void firstExtent(const Extent &b_e) {
        ExtentSeries a_series;

        a_series.setExtent(a_input.getExtent());
        b_series.setType(b_e.getType());
        if (a_series.getExtent() == NULL) {
            throw RequestError("a_table is empty?");
        }

        vector<GeneralField::Ptr> a_eq_fields;
        HashMap<string, GeneralField::Ptr> a_name_to_b_field, b_name_to_b_field;

        BOOST_FOREACH(const CMap::value_type &vt, eq_columns) {
            SINVARIANT(a_series.getType()->getFieldType(vt.first)
                       == b_series.getType()->getFieldType(vt.second));
            a_eq_fields.push_back(GeneralField::make(a_series, vt.first));
            b_eq_fields.push_back(GeneralField::make(b_series, vt.second));
            a_name_to_b_field[vt.first] = b_eq_fields.back();
            b_name_to_b_field[vt.second] = b_eq_fields.back();
        }

        vector<GeneralField::Ptr> a_val_fields;
        HashMap<string, uint32_t> a_name_to_val_pos;
        BOOST_FOREACH(const CMap::value_type &vt, keep_columns) {
            if (prefixequal(vt.first, "a.")) {
                string field_name(vt.first.substr(2));
                if (!a_name_to_b_field.exists(field_name)) { // value only
                    a_name_to_val_pos[field_name] = a_val_fields.size();
                    a_val_fields.push_back(GeneralField::make(a_series, field_name));
                }
            }
        }
        string output_xml(str(format("<ExtentType name=\"hash-join -> %s\""
                                     " namespace=\"server.example.com\" version=\"1.0\">\n")
                              % output_table_name));

        // Two possible sources for values in the output, either it comes from stored a values
        // or it comes from a b field, either one of the eq fields or a value field.
        BOOST_FOREACH(const CMap::value_type &vt, keep_columns) {
            string field_name(vt.first.substr(2));
            string output_field_type;
            if (prefixequal(vt.first, "a.")) {
                SINVARIANT(a_series.getType()->hasColumn(field_name));
                output_field_type = a_series.getType()->getFieldTypeStr(field_name);
                if (a_name_to_b_field.exists(field_name)) { // extract from the b eq fields.
                    const GeneralField::Ptr b_field(a_name_to_b_field.dGet(field_name));
                    SINVARIANT(b_field != NULL);
                    extractors.push_back(ExtractorField::make(vt.second, b_field));
                } else { // extract from the a values
                    SINVARIANT(a_name_to_val_pos.exists(field_name));
                    extractors.push_back
                        (ExtractorValue::make(vt.second, a_name_to_val_pos[field_name]));
                }
            } else if (prefixequal(vt.first, "b.")) {
                SINVARIANT(b_series.getType()->hasColumn(field_name));
                output_field_type = b_series.getType()->getFieldTypeStr(field_name);
                if (b_name_to_b_field.exists(field_name)) { // extract from the b eq fields.
                    const GeneralField::Ptr b_field(b_name_to_b_field.dGet(field_name));
                    SINVARIANT(b_field != NULL);
                    extractors.push_back(ExtractorField::make(vt.second, b_field));
                } else { // extract from b series
                    GeneralField::Ptr b_field(GeneralField::make(b_series, field_name));
                    extractors.push_back(ExtractorField::make(vt.second, b_field));
                }
            }
            if (output_field_type.empty()) continue; // HACK
            output_xml.append(str(format("  <field type=\"%s\" name=\"%s\" />\n")
                                  % output_field_type % vt.second));
        }
        output_xml.append("</ExtentType>\n");
                    
        INVARIANT(!extractors.empty(), "must extract at least one field");
        int32_t row_count = 0;
        key.resize(a_eq_fields.size());
        GVVec val;
        val.resize(a_val_fields.size());
        while (1) {
            if (a_series.getExtent() == NULL) {
                break;
            }
            for(; a_series.more(); a_series.next()) {
                ++row_count;

                if (row_count >= max_a_rows) {
                    throw RequestError("a table has too many rows");
                }
                extractGVVec(a_eq_fields, key);
                extractGVVec(a_val_fields, val);
                a_hashmap[key].push_back(val);
            }
            delete a_series.getExtent();
            a_series.setExtent(a_input.getExtent());
        }

        ExtentTypeLibrary lib;
        LintelLog::info(format("output xml: %s") % output_xml);
        output_series.setType(lib.registerTypeR(output_xml));
        output_extent = new Extent(*output_series.getType());
        
        BOOST_FOREACH(Extractor::Ptr p, extractors) {
            p->into = GeneralField::make(output_series, p->field_name);
        }
    }

    virtual Extent *getExtent() {
        while (true) {
            Extent *e = b_input.getExtent();
            if (e == NULL) {
                break;
            }
            if (output_series.getType() == NULL) {
                firstExtent(*e);
            }
        
            if (output_series.getExtent() == NULL) {
                Extent *output_extent = new Extent(*output_series.getType());
                output_series.setExtent(output_extent); 
            }
            for (b_series.setExtent(e); b_series.more(); b_series.next()) {
                processRow();
            }
            if (output_series.getExtent()->size() > 96*1024) {
                break;
            }
        }
        Extent *ret = output_series.getExtent();
        output_series.clearExtent();
        return ret;
    }

    void processRow() {
        extractGVVec(b_eq_fields, key);
        vector<GVVec> *v = a_hashmap.lookup(key);
        if (v != NULL) {
            cout << format("%d match on %s\n") % v->size() % key;
            BOOST_FOREACH(const GVVec &a_vals, *v) {
                output_series.newRecord();
                BOOST_FOREACH(Extractor::Ptr p, extractors) {
                    p->extract(a_vals);
                }
            }
        }
    }

    static void extractGVVec(vector<GeneralField::Ptr> &fields, GVVec &into) {
        SINVARIANT(fields.size() == into.size());
        for (uint32_t i = 0; i < fields.size(); ++i) {
            into[i].set(fields[i]);
        }
    }
    
    DataSeriesModule &a_input, &b_input;
    int32_t max_a_rows;
    const CMap eq_columns, keep_columns;
    
    HashMap< GVVec, vector<GVVec> > a_hashmap;
    GVVec key;
    ExtentSeries b_series, output_series;
    vector<GeneralField::Ptr> b_eq_fields;
    vector<Extractor::Ptr> extractors;
    Extent *output_extent;
    const string output_table_name;
    
};

class DataSeriesServerHandler : public DataSeriesServerIf {
public:
    struct TableInfo {
	string extent_type;
	vector<string> depends_on;
	Clock::Tfrac last_update;
	TableInfo() : extent_type(), last_update(0) { }
    };

    typedef HashMap<string, TableInfo> NameToInfo;

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
        
    void importData(const string &dest_table, const string &xml_desc, const TableData &data) {
        verifyTableName(dest_table);

        if (data.more_rows) {
            throw RequestError("can not handle more rows");
        }
        ExtentTypeLibrary lib;
        const ExtentType &type(lib.registerTypeR(xml_desc));

        ExtentSeries output_series(type);
        DataSeriesSink output_sink(tableToPath(dest_table), Extent::compress_lzf, 1);
        OutputModule output_module(output_sink, output_series, type, 96*1024);

        output_sink.writeExtentLibrary(lib);

        vector<boost::shared_ptr<GeneralField> > fields;
        for (uint32_t i = 0; i < type.getNFields(); ++i) {
            boost::shared_ptr<GeneralField> 
                tmp(GeneralField::create(output_series, type.getFieldName(i)));
            fields.push_back(tmp);
        }

        BOOST_FOREACH(const vector<string> &row, data.rows) {
            output_module.newRecord();
            if (row.size() != fields.size()) {
                throw new RequestError("incorrect number of fields");
            }
            for (uint32_t i = 0; i < row.size(); ++i) {
                fields[i]->set(row[i]);
            }
        }

        updateTableInfo(dest_table, type.getName());
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
        NameToInfo::iterator i = getTableInfo(source_table);

        TypeIndexModule input(i->second.extent_type);
        input.addSource(tableToPath(source_table));
        TableDataModule sink(input, ret, max_rows);

        sink.getAndDelete();
    }

    void hashJoin(const string &a_table, const string &b_table, const string &out_table,
                  const map<string, string> &eq_columns, 
                  const map<string, string> &keep_columns, int32_t max_a_rows) { 
        NameToInfo::iterator a_info = getTableInfo(a_table);
        NameToInfo::iterator b_info = getTableInfo(b_table);

        verifyTableName(out_table);

        TypeIndexModule a_input(a_info->second.extent_type);
        a_input.addSource(tableToPath(a_table));
        TypeIndexModule b_input(b_info->second.extent_type);
        b_input.addSource(tableToPath(b_table));

        HashJoinModule hj_module(a_input, max_a_rows, b_input, eq_columns, keep_columns,
                                 out_table);

        TeeModule output_module(hj_module, tableToPath(out_table));
        
        output_module.getAndDelete();
        updateTableInfo(out_table, hj_module.output_series.getType()->getName());
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

    NameToInfo::iterator getTableInfo(const string &table_name) {
        NameToInfo::iterator ret = table_info.find(table_name);
        if (ret == table_info.end()) {
            throw TApplicationException("table missing");
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
