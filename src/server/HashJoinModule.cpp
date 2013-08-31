#include "ServerModules.hpp"
#include "DSSModule.hpp"
#include "JoinModule.hpp"

// TODO: merge common code with StarJoinModule

class HashJoinModule : public JoinModule { 
  public:
    typedef map<string, string> CMap; // column map

    HashJoinModule(DataSeriesModule &a_input, int32_t max_a_rows, DataSeriesModule &b_input,
                   const map<string, string> &eq_columns, const map<string, string> &keep_columns,
                   const string &output_table_name) 
            : a_input(a_input), b_input(b_input), max_a_rows(max_a_rows), 
              eq_columns(eq_columns), keep_columns(keep_columns), 
              output_table_name(output_table_name)
    { }

    static const string mapDGet(const map<string, string> &a_map, const string &a_key) {
        map<string, string>::const_iterator i = a_map.find(a_key);
        SINVARIANT(i != a_map.end());
        return i->second;
    }

    void firstExtent(const Extent &b_e) {
        ExtentSeries a_series;

        a_series.setExtent(a_input.getSharedExtent());
        b_series.setType(b_e.getTypePtr());
        if (a_series.getSharedExtent() == NULL) {
            requestError("a_table is empty?");
        }

        // Three possible sources for values in the output:
        // 
        // 1) the a value fields, so from the hash-map
        // 2a) the b fields, as one of the a eq fields.
        // 2b) the b fields, as one of the b values or eq fields
        // 
        // We do not try to optimize the extraction and just create a new general field for each
        // extraction so if the hash-map has duplicate columns, there will be duplicate fields.
        vector<GeneralField::Ptr> a_eq_fields;
        HashUnique<string> known_a_eq_fields;

        BOOST_FOREACH(const CMap::value_type &vt, eq_columns) {
            SINVARIANT(a_series.getTypePtr()->getFieldType(vt.first)
                       == b_series.getTypePtr()->getFieldType(vt.second));
            a_eq_fields.push_back(GeneralField::make(a_series, vt.first));
            b_eq_fields.push_back(GeneralField::make(b_series, vt.second));
            known_a_eq_fields.add(vt.first);
        }

        vector<GeneralField::Ptr> a_val_fields;
        HashMap<string, uint32_t> a_name_to_val_pos;

        BOOST_FOREACH(const CMap::value_type &vt, keep_columns) {
            if (prefixequal(vt.first, "a.")) {
                string field_name(vt.first.substr(2));
                if (!known_a_eq_fields.exists(field_name)) { // don't store eq fields we will
                    // access from the b eq fields
                    a_name_to_val_pos[field_name] = a_val_fields.size();
                    a_val_fields.push_back(GeneralField::make(a_series, field_name));
                }
            }
        }
        string output_xml(str(format("<ExtentType name=\"hash-join -> %s\""
                                     " namespace=\"server.example.com\" version=\"1.0\">\n")
                              % output_table_name));

        BOOST_FOREACH(const CMap::value_type &vt, keep_columns) {
            string field_name(vt.first.substr(2));
            string output_field_xml;

            if (prefixequal(vt.first, "a.") && a_name_to_val_pos.exists(field_name)) { // case 1
                TINVARIANT(a_series.getTypePtr()->hasColumn(field_name));
                output_field_xml = renameField(a_series.getTypePtr(), field_name, vt.second);
                extractors.push_back
                        (ExtractorValue::make(vt.second, a_name_to_val_pos[field_name]));
            } else if (prefixequal(vt.first, "a.") 
                       && eq_columns.find(field_name) != eq_columns.end()) { // case 2a
                const string b_field_name(eq_columns.find(field_name)->second);
                TINVARIANT(b_series.getTypePtr()->hasColumn(b_field_name));
                output_field_xml = renameField(a_series.getTypePtr(), field_name, vt.second);
                GeneralField::Ptr b_field(GeneralField::make(b_series, b_field_name));
                extractors.push_back(ExtractorField::make(vt.second, b_field));
            } else if (prefixequal(vt.first, "b.")
                       && b_series.getTypePtr()->hasColumn(field_name)) { // case 2b
                output_field_xml = renameField(b_series.getTypePtr(), field_name, vt.second);
                GeneralField::Ptr b_field(GeneralField::make(b_series, field_name));
                extractors.push_back(ExtractorField::make(vt.second, b_field));
            } else {
                requestError("invalid extraction");
            }
            output_xml.append(output_field_xml);
        }

        output_xml.append("</ExtentType>\n");
                    
        INVARIANT(!extractors.empty(), "must extract at least one field");
        int32_t row_count = 0;
        key.resize(a_eq_fields.size());
        GVVec val;
        val.resize(a_val_fields.size());
        while (1) {
            if (a_series.getSharedExtent() == NULL) {
                break;
            }
            for (; a_series.more(); a_series.next()) {
                ++row_count;

                if (row_count >= max_a_rows) {
                    requestError("a table has too many rows");
                }
                key.extract(a_eq_fields);
                val.extract(a_val_fields);
                a_hashmap[key].push_back(val);
            }
            a_series.setExtent(a_input.getSharedExtent());
        }

        ExtentTypeLibrary lib;
        LintelLog::info(format("output xml: %s") % output_xml);
        output_series.setType(lib.registerTypePtr(output_xml));
        
        Extractor::makeInto(extractors, output_series);
    }

    virtual Extent::Ptr getSharedExtent() {
        while (true) {
            Extent::Ptr e = b_input.getSharedExtent();
            if (e == NULL) {
                break;
            }
            if (output_series.getTypePtr() == NULL) {
                firstExtent(*e);
            }
        
            if (output_series.getSharedExtent() == NULL) {
                SINVARIANT(output_series.getTypePtr() != NULL);
                output_series.newExtent();
            }
            for (b_series.setExtent(e); b_series.more(); b_series.next()) {
                processRow();
            }

            if (output_series.getSharedExtent()->size() > 96*1024) {
                break;
            }
        }
        return returnOutputSeries();
    }

    void processRow() {
        key.extract(b_eq_fields);
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

    DataSeriesModule &a_input, &b_input;
    int32_t max_a_rows;
    const CMap eq_columns, keep_columns;
    
    HashMap< GVVec, vector<GVVec> > a_hashmap;
    GVVec key;
    ExtentSeries b_series;
    vector<GeneralField::Ptr> b_eq_fields;
    vector<Extractor::Ptr> extractors;
    const string output_table_name;
    
};

OutputSeriesModule::OSMPtr dataseries::makeHashJoinModule
(DataSeriesModule &a_input, int32_t max_a_rows, DataSeriesModule &b_input,
 const map<string, string> &eq_columns, const map<string, string> &keep_columns,
 const string &output_table_name) {
    return OutputSeriesModule::OSMPtr(new HashJoinModule(a_input, max_a_rows, b_input, eq_columns,
                                                         keep_columns, output_table_name));
}

