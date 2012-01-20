#include <Lintel/PriorityQueue.hpp>

#include "DSSModule.hpp"

class UnionModule : public OutputSeriesModule {
public:
    struct Compare {
        Compare(const vector<UM_UnionTable> &sources) : sources(sources) { }
        bool operator()(uint32_t ia, uint32_t ib) {
            const UM_UnionTable &a(sources[ia]);
            const UM_UnionTable &b(sources[ib]);
            SINVARIANT(a.series.getExtent() != NULL);
            SINVARIANT(b.series.getExtent() != NULL);
            SINVARIANT(a.order_fields.size() == b.order_fields.size());
            for (size_t i = 0; i < a.order_fields.size(); ++i)  {
                if (*a.order_fields[i] < *b.order_fields[i]) {
                    return false;
                } else if (*b.order_fields[i] < *a.order_fields[i]) {
                    return true;
                }
            }
            // They are equal, order by union position
            return ia >= ib;
        }
    private:
        const vector<UM_UnionTable> &sources;
    };

    UnionModule(const vector<UM_UnionTable> &in_sources, const vector<string> &order_columns,
                const string &output_table_name) 
        : sources(in_sources), compare(sources), queue(compare, sources.size()),
          order_columns(order_columns), output_table_name(output_table_name)
    {
        SINVARIANT(!sources.empty());
    }

    virtual ~UnionModule() { }

    void firstExtent() {
        map<string, string> output_name_to_type;
        typedef map<string, string>::value_type ss_vt;

        BOOST_FOREACH(UM_UnionTable &ut, sources) {
            SINVARIANT(ut.copier == NULL);
            ut.copier.reset(new RenameCopier(ut.series, output_series));

            SINVARIANT(ut.source != NULL);
            ut.series.setExtent(ut.source->getExtent());
            if (ut.series.getExtent() == NULL) {
                continue; // no point in making the other bits
            }
            
            map<string, string> out_to_in;
            BOOST_FOREACH(const ss_vt &v, ut.extract_values) {
                out_to_in[v.second] = v.first;
                string &output_type(output_name_to_type[v.second]);
                string renamed_output_type(renameField(ut.series.getType(), v.first, v.second));
                if (output_type.empty()) {
                    output_type = renamed_output_type;
                } else {
                    TINVARIANT(output_type == renamed_output_type);
                }
            }
            BOOST_FOREACH(const string &col, order_columns) {
                map<string, string>::iterator i = out_to_in.find(col);
                SINVARIANT(i != out_to_in.end());
                ut.order_fields.push_back(GeneralField::make(ut.series, i->second));
            }
            LintelLogDebug("UnionModule", format("made union stuff on %s") % ut.table_name);
        }

        string output_xml(str(format("<ExtentType name=\"union -> %s\""
                                     " namespace=\"server.example.com\" version=\"1.0\">\n")
                              % output_table_name));

        BOOST_FOREACH(const ss_vt &v, output_name_to_type) {
            output_xml.append(v.second);
        }
        output_xml.append("</ExtentType>\n");
        LintelLogDebug("UnionModule", format("constructed output type:\n%s") % output_xml);
        ExtentTypeLibrary lib;
        output_series.setType(lib.registerTypeR(output_xml));
        for (uint32_t i = 0; i < sources.size(); ++i) {
            UM_UnionTable &ut(sources[i]);
            if (ut.series.getExtent() != NULL) {
                ut.copier->prep(ut.extract_values);
                queue.push(i);
            }
        }
    }

    virtual Extent *getExtent() {
        if (sources[0].copier == NULL) {
            firstExtent();
        }

        while (true) {
            if (queue.empty()) {
                break;
            }
            uint32_t best = queue.top();

            LintelLogDebug("UnionModule/process", format("best was %d") % best);

            if (output_series.getExtent() == NULL) {
                output_series.setExtent(new Extent(*output_series.getType()));
            }

            output_series.newRecord();
            sources[best].copier->copyRecord();
            ++sources[best].series;
            if (!sources[best].series.more()) {
                delete sources[best].series.getExtent();
                sources[best].series.setExtent(sources[best].source->getExtent());
            }
            if (sources[best].series.getExtent() == NULL) {
                queue.pop();
            } else {
                queue.replaceTop(best); // it's position may have changed, re-add
            }
            if (output_series.getExtent()->size() > 96*1024) {
                break;
            }
        }

        return returnOutputSeries();
    }

    vector<UM_UnionTable> sources; 
    Compare compare;
    PriorityQueue<uint32_t, Compare> queue;
    vector<string> order_columns;
    const string output_table_name;
};

OutputSeriesModule::OSMPtr dataseries::makeUnionModule
    (const vector<UM_UnionTable> &in_sources, const vector<string> &order_columns,
     const string &output_table_name) {
    return OutputSeriesModule::OSMPtr(new UnionModule(in_sources, order_columns,
                                                      output_table_name));
}

