#include "DSSModule.hpp"
#include "JoinModule.hpp"

// In some ways this is very similar to the HashJoinModule, however one of the common data
// warehousing operations is to scan through a fact table adding in new columns based on one or
// more tiny dimension tables.  The efficient implementation is to load all of those tiny tables
// and make a single pass through the big table combining it with all of the tiny tables.
//
// To do that we will define a vector of Dimension(name, table, key = vector<column>, values =
// vector<column>) entries that define all of the dimension tables along with the key indexing into
// that dimension table, and a LookupIn(name, key = vector<column>, missing -> { skip_row |
// unchanged | value }, map<int, string> extract_values) that will lookup in JoinTo(name) using the
// two keys, and extract out a subset of the values from the JoinTo into the named columns.
// If the lookup is missing, it can either cause us to skip that row in the fact table, leave the
// output values unchanged, or set it to a specified value.  The middle option allows for lookups
// in multiple tables to select the last set value.
class StarJoinModule : public JoinModule {
public:
    StarJoinModule(DataSeriesModule &fact_input, const vector<Dimension> &dimensions,
                   const string &output_table_name, const map<string, string> &fact_columns,
                   const vector<DimensionFactJoin> &dimension_fact_join_in,
                   const HashMap< string, shared_ptr<DataSeriesModule> > &dimension_modules) 
        : fact_input(fact_input), dimensions(dimensions), output_table_name(output_table_name),
          fact_column_names(fact_columns), dimension_modules(dimension_modules)
    { 
        BOOST_FOREACH(const DimensionFactJoin &dfj, dimension_fact_join_in) {
            SJM_Join::Ptr p(new SJM_Join(dfj));
            dimension_fact_join.push_back(p);
        }
    }

    struct SJM_Dimension : public Dimension {
        typedef boost::shared_ptr<SJM_Dimension> Ptr;

        SJM_Dimension(Dimension &d) : Dimension(d), dimension_type() { }

        size_t valuePos(const string &source_field_name) {
            for(size_t i = 0; i < value_columns.size(); ++i) {
                if (value_columns[i] == source_field_name) {
                    return i;
                }
            }
            return numeric_limits<size_t>::max();
        }

        const ExtentType *dimension_type;
        HashMap<GVVec, GVVec> dimension_data; // map dimension key to dimension values
    };

    struct SJM_Join : public DimensionFactJoin {
        typedef boost::shared_ptr<SJM_Join> Ptr;
        
        SJM_Join(const DimensionFactJoin &d) : DimensionFactJoin(d), dimension(), join_data() { }
        virtual ~SJM_Join() throw () { }
        SJM_Dimension::Ptr dimension;
        vector<GeneralField::Ptr> fact_fields;
        vector<Extractor::Ptr> extractors;
        GVVec join_key; // avoid constant resizes, just overwrite
        GVVec *join_data; // temporary for the two-phase join
    };

    class DimensionModule : public RowAnalysisModule {
    public:
        typedef boost::shared_ptr<DimensionModule> Ptr;

        static Ptr make(DataSeriesModule &source, SJM_Dimension &sjm_dimension) {
            Ptr ret(new DimensionModule(source, sjm_dimension));
            return ret;
        }

        void firstExtent(const Extent &e) {
            LintelLogDebug("StarJoinModule/DimensionModule", format("first extent for %s via %s")
                           % dim.dimension_name % dim.source_table);
            key_gv.resize(dim.key_columns.size());
            value_gv.resize(dim.value_columns.size());
            series.setType(e.getType());
            BOOST_FOREACH(string key, dim.key_columns) {
                key_gf.push_back(GeneralField::make(series, key));
            }
            BOOST_FOREACH(string value, dim.value_columns) {
                value_gf.push_back(GeneralField::make(series, value));
            }
            SINVARIANT(dim.dimension_type == NULL);
            dim.dimension_type = series.getType();
        }

        void processRow() {
            key_gv.extract(key_gf);
            value_gv.extract(value_gf);
            dim.dimension_data[key_gv] = value_gv;
            LintelLogDebug("StarJoinModule/Dimension", format("[%d] -> [%d]") % key_gv % value_gv);
        }

    protected:
        DimensionModule(DataSeriesModule &source, SJM_Dimension &sjm_dimension) 
            : RowAnalysisModule(source), dim(sjm_dimension)
        { }

        SJM_Dimension &dim;

        vector<GeneralField::Ptr> key_gf, value_gf;
        GVVec key_gv, value_gv;
    };
            
    void processDimensions() {
        // TODO: probably could do this in only two loops rather than three, but this
        // is simpler for now, so leave it alone.
        name_to_sjm_dim.reserve(dimensions.size());
        BOOST_FOREACH(const SJM_Join::Ptr dfj, dimension_fact_join) {
            name_to_sjm_dim[dfj->dimension_name].reset(); // mark used dimensions
        }
        BOOST_FOREACH(Dimension &dim, dimensions) {
            if (name_to_sjm_dim.exists(dim.dimension_name)) {
                name_to_sjm_dim[dim.dimension_name].reset(new SJM_Dimension(dim));
            } else {
                requestError(format("unused dimension %s specified") % dim.dimension_name);
            }
        }

        BOOST_FOREACH(const SJM_Join::Ptr dfj, dimension_fact_join) {
            dfj->dimension = name_to_sjm_dim[dfj->dimension_name];
            if (dfj->dimension == NULL) {
                requestError(format("unspecified dimension %s used") % dfj->dimension_name);
            }
        }

        // TODO: check for identical source table + same keys, never makes sense, better to
        // just pull multiple values in that case.
        BOOST_FOREACH(string source_table, dimension_modules.keys()) {
            // Will be reading same extent multiple times for different dimensions. Little bit
            // inefficient but easy to implement.
            SequenceModule seq(dimension_modules[source_table]);
            BOOST_FOREACH(Dimension &dim, dimensions) {
                if (dim.source_table == source_table) {
                    LintelLogDebug("StarJoinModule", format("loading %s from %s") 
                                   % dim.dimension_name % dim.source_table);
                    seq.addModule(DimensionModule::make(seq.tail(), 
                                                        *name_to_sjm_dim[dim.dimension_name]));
                }
            }

            if (seq.size() == 1) {
                requestError(format("Missing dimensions using source table %s") % source_table);
            }
            seq.getAndDeleteShared();
        }
        
        dimension_modules.clear();
    }

    void setOutputType() {
        SINVARIANT(fact_series.getType() != NULL);
        string output_xml(str(format("<ExtentType name=\"star-join -> %s\""
                                     " namespace=\"server.example.com\" version=\"1.0\">\n")
                              % output_table_name));
        typedef map<string, string>::value_type ss_pair;
        BOOST_FOREACH(ss_pair &v, fact_column_names) {
            TINVARIANT(fact_series.getType()->hasColumn(v.first));
            output_xml.append(renameField(fact_series.getType(), v.first, v.second));
            fact_fields.push_back(ExtractorField::make(fact_series, v.first, v.second));
        }
        BOOST_FOREACH(const SJM_Join::Ptr dfj, dimension_fact_join) {
            SJM_Dimension::Ptr dim = name_to_sjm_dim[dfj->dimension_name];
            SINVARIANT(dim->dimension_type != NULL);

            dfj->join_key.resize(dfj->fact_key_columns.size());
            BOOST_FOREACH(const string &fact_col_name, dfj->fact_key_columns) {
                dfj->fact_fields.push_back(GeneralField::make(fact_series, fact_col_name));
            }
            BOOST_FOREACH(ss_pair &ev, dfj->extract_values) {
                TINVARIANT(dim->dimension_type->hasColumn(ev.first));
                size_t value_pos = dim->valuePos(ev.first);
                TINVARIANT(value_pos != numeric_limits<size_t>::max());
                output_xml.append(renameField(dim->dimension_type, ev.first, ev.second));
                dfj->extractors.push_back(ExtractorValue::make(ev.second, value_pos));
            }
        }
        output_xml.append("</ExtentType>\n");
        LintelLogDebug("StarJoinModule", format("constructed output type:\n%s") % output_xml);

        ExtentTypeLibrary lib;
        output_series.setType(lib.registerTypeR(output_xml));

        Extractor::makeInto(fact_fields, output_series);
        BOOST_FOREACH(const SJM_Join::Ptr dfj, dimension_fact_join) {
            Extractor::makeInto(dfj->extractors, output_series);
        }
    }

    void firstExtent(const Extent &fact_extent) {
        processDimensions();
        fact_series.setType(fact_extent.getType());
        setOutputType();
    }

    virtual Extent *getExtent() {
        scoped_ptr<Extent> e(fact_input.getExtent());
        if (e == NULL) {
            return NULL;
        }
        if (output_series.getType() == NULL) {
            firstExtent(*e);
        }
        
        output_series.setExtent(new Extent(output_series));
        // TODO: do the resize to ~64-96k trick here rather than one in one out.
        for (fact_series.setExtent(e.get()); fact_series.more(); fact_series.next()) {
            processRow();
        }

        Extent *ret = output_series.getExtent();
        output_series.clearExtent();
        return ret;
    }

    void processRow() {
        // Two phases, in the first phase we look up everything and verify that we're going
        // to generate an output row.   In the second phase we generate the output row.

        BOOST_FOREACH(SJM_Join::Ptr join, dimension_fact_join) {
            join->join_key.extract(join->fact_fields);
            HashMap<GVVec, GVVec>::iterator i 
                = join->dimension->dimension_data.find(join->join_key);
            if (i == join->dimension->dimension_data.end()) {
                FATAL_ERROR(format("unimplemented; unable to find key '%s' in dimension '%s'")
                            % join->join_key % join->dimension->dimension_name);
            } else {
                SINVARIANT(join->join_data == NULL);
                join->join_data = &i->second;
            }
        }

        // All dimensions exist, make an output row.
        GVVec empty;
        output_series.newRecord();
        Extractor::extractAll(fact_fields, empty);
        BOOST_FOREACH(SJM_Join::Ptr join, dimension_fact_join) {
            SINVARIANT(join->join_data != NULL);
            Extractor::extractAll(join->extractors, *join->join_data);
            join->join_data = NULL;
        }
    }

    DataSeriesModule &fact_input;
    vector<Dimension> dimensions;
    string output_table_name;
    map<string, string> fact_column_names;
    ExtentSeries fact_series;
    vector<SJM_Join::Ptr> dimension_fact_join;
    HashMap< string, shared_ptr<DataSeriesModule> > dimension_modules; // source table to typeindexmodule
    HashMap<string, SJM_Dimension::Ptr> name_to_sjm_dim; // dimension name to SJM dimension
    vector<Extractor::Ptr> fact_fields;
};    

OutputSeriesModule::OSMPtr dataseries::makeStarJoinModule
  (DataSeriesModule &fact_input, const vector<Dimension> &dimensions,
   const string &output_table_name,
   const map<string, string> &fact_columns,
   const vector<DimensionFactJoin> &dimension_fact_join_in,
   const HashMap< string, boost::shared_ptr<DataSeriesModule> > &dimension_modules) {
    return OutputSeriesModule::OSMPtr(new StarJoinModule(fact_input, dimensions, output_table_name,
                                                         fact_columns, dimension_fact_join_in,
                                                         dimension_modules));
}
