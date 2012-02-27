#include "DSSModule.hpp"

#include <new> // losertree.h needs this and forgot to include it.
#include <parallel/losertree.h>

/* Note: there are several versions of sort modules on the tomer sub-branch.  There is an in-memory
   sort, a radix in memory sort, a spilling to disk sort and a parallel sort.  The latter versions
   are somewhat specific to the sorting rules needed for the sort benchmark.  The former is a
   complicated template, and does not support sorting based on multiple columns.  Since the intent
   in this code is to first make slow, generalfield implementations and then eventually dynamically
   generate C++ source code for specific operations, we don't need the complication of the
   template, and so we create yet another module to be more in the style of the server */

#if 0
#include <algorithm>
#include <vector>

// Note the STL algorithm chooses to copy the comparator; the following program will abort on some
// (most/all?) STL implementations, which means that implementing a comparator with state is
// inherently less efficient unless the compilers value propagation does a really good job.

using namespace std;

class Comparator {
public:
    Comparator() { }

    Comparator(const Comparator &) { abort(); }

    Comparator &operator =(const Comparator &) { abort(); }
    
    bool operator ()(const int a, const int b) { return a < b; }
};

int main() {
    vector<int> a;
    a.push_back(1);
    a.push_back(1);
    a.push_back(1);
    a.push_back(1);
    a.push_back(1);

    sort(a.begin(), a.end(), Comparator());
}
#endif

class SortModule : public OutputSeriesModule {
public:
    SortModule(DataSeriesModule &source, const vector<SortColumn> &sort_by)
        : source(source), sort_by(sort_by), copier(input_series, output_series), ercs(),
          sorted_extents(), merge()
    { }

    virtual ~SortModule() { }

    struct ExtentRowCompareState {
        ExtentRowCompareState() : extent(), columns() { }
        Extent::Ptr extent;
        vector<SortColumnImpl> columns;
    };

    static bool strictlyLessThan(const Extent::Ptr &ea, const SEP_RowOffset &oa,
                                 const Extent::Ptr &eb, const SEP_RowOffset &ob,
                                 const vector<SortColumnImpl> &columns) {
            BOOST_FOREACH(const SortColumnImpl &c, columns) {
                bool a_null = c.field->isNull(*ea, oa);
                bool b_null = c.field->isNull(*eb, ob);

                if (a_null || b_null) {
                    if (a_null && !b_null) { //         a < b : a > b
                        return c.null_mode == NM_First ? true : false;
                    } else if (!a_null && b_null) { //   a > b : a < b
                        return c.null_mode == NM_First ? false : true;
                    } else {
                        // ==; keep going
                    }
                } else {
                    GeneralValue va(c.field->val(*ea, oa));
                    GeneralValue vb(c.field->val(*eb, ob));
                    if (va < vb) {
                        return c.sort_less;
                    } else if (vb < va) {
                        return !c.sort_less;
                    } else {
                        // ==; keep going
                    }
                }
            }
            return false; // all == so not <
    }

    class ExtentRowCompare {
    public:
        ExtentRowCompare(ExtentRowCompareState *state) : state(state) { }
        ExtentRowCompare(const ExtentRowCompare &from) : state(from.state) { }
        ExtentRowCompare &operator =(const ExtentRowCompare &rhs) {
            state = rhs.state;
            return *this;
        }

        bool operator ()(const SEP_RowOffset &a, const SEP_RowOffset &b) const {
            return strictlyLessThan(state->extent, a, state->extent, b, state->columns);
        }

        const ExtentRowCompareState *state;
    };
        
    struct SortedExtent {
        SortedExtent(Extent::Ptr e) : e(e), offsets(), pos() { }
        typedef boost::shared_ptr<SortedExtent> Ptr;

        Extent::Ptr e;
        vector<SEP_RowOffset> offsets;
        vector<SEP_RowOffset>::iterator pos;
    };
    
    struct LoserTreeCompare {
        LoserTreeCompare(SortModule *sm) : sm(sm) { }
        bool operator()(uint32_t ia, uint32_t ib) const {
            const SortedExtent &sea(*sm->sorted_extents[ia]);
            const SortedExtent &seb(*sm->sorted_extents[ib]);
            return strictlyLessThan(sea.e, *sea.pos, seb.e, *seb.pos, sm->ercs.columns);
        }

        const SortModule *sm;
    };        

    typedef __gnu_parallel::LoserTree<true, uint32_t, LoserTreeCompare> LoserTree;

    void firstExtent(Extent &in) {
        const ExtentType &t(in.getType());
        input_series.setType(t);
        output_series.setType(t);

        copier.prep();

        BOOST_FOREACH(SortColumn &by, sort_by) {
            TINVARIANT(by.sort_mode == SM_Ascending || by.sort_mode == SM_Decending);
            TINVARIANT(by.null_mode == NM_First || by.null_mode == NM_Last);
            ercs.columns.push_back(SortColumnImpl(GeneralField::make(input_series, by.column),
                                                  by.sort_mode == SM_Ascending ? true : false,
                                                  by.null_mode));
        }
    }

    void sortExtent(Extent::Ptr in) {
        ercs.extent = in;
        SortedExtent::Ptr se(new SortedExtent(in));
        se->offsets.reserve(in->nRecords());
        for (input_series.setExtent(in); input_series.more(); input_series.next()) {
            se->offsets.push_back(input_series.getRowOffset());
        }

        stable_sort(se->offsets.begin(), se->offsets.end(), ExtentRowCompare(&ercs));

        ercs.extent.reset();
        sorted_extents.push_back(se);
    }

    Extent::Ptr processSingleMerge() {
        output_series.newExtent();
        // loser tree gets this case wrong, and goes into an infinite loop (log_2(0) is a bad
        // idea with a check 0 * 2^n > 0.
        SortedExtent &se(*sorted_extents[0]);
        for (se.pos = se.offsets.begin(); se.pos != se.offsets.end(); ++se.pos) {
            output_series.newRecord();
            copier.copyRecord(*se.e, *se.pos);
        }
        sorted_extents.clear();
        return returnOutputSeries();
    }

    Extent::Ptr cleanupMultiMerge() {
        BOOST_FOREACH(SortedExtent::Ptr p, sorted_extents) {
            SINVARIANT(p->pos == p->offsets.end());
        }
        sorted_extents.clear();
        return returnOutputSeries();
    }

    Extent::Ptr processMerge() {
        LintelLogDebug("SortModule", "doing multi-extent-merge");
        output_series.newExtent();
        while (true) {
            int min = merge->get_min_source();
            if (min < 0 || static_cast<size_t>(min) >= sorted_extents.size()) {
                // loser tree exit path 1
                return cleanupMultiMerge();
            }
            SortedExtent &se(*sorted_extents[min]);
            if (se.pos == se.offsets.end()) {
                // loser tree exit path 2
                return cleanupMultiMerge();
            }

            output_series.newRecord();
            copier.copyRecord(*se.e, *se.pos);
            ++se.pos;
            merge->delete_min_insert(min, se.pos == se.offsets.end());
            if (output_series.getExtentRef().size() > 96*1024) {
                break;
            }
        }
        return returnOutputSeries();
    }

    virtual Extent::Ptr getSharedExtent() {
        if (sorted_extents.empty()) {
            while (true) {
                Extent::Ptr in = source.getSharedExtent();
                if (in == NULL) {
                    break;
                }
                if (input_series.getType() == NULL) {
                    firstExtent(*in);
                }
                
                sortExtent(in);
            }

            if (sorted_extents.empty()) {
                return Extent::Ptr();
            } else if (sorted_extents.size() == 1) {
                return processSingleMerge();
            } else {
                merge = new LoserTree(sorted_extents.size(), LoserTreeCompare(this));

                for (uint32_t i = 0; i < sorted_extents.size(); ++i) {
                    merge->insert_start(i, i, false);
                    sorted_extents[i]->pos = sorted_extents[i]->offsets.begin();
                }
                merge->init();
            }
        } 

        return processMerge();
    }
        
    DataSeriesModule &source;
    vector<SortColumn> sort_by;
    ExtentSeries input_series;
    ExtentRecordCopy copier;
    ExtentRowCompareState ercs;
    vector<SortedExtent::Ptr> sorted_extents;
    LoserTree *merge;
};

OutputSeriesModule::OSMPtr dataseries::makeSortModule
(DataSeriesModule &source, const vector<SortColumn> &sort_by) {
    return OutputSeriesModule::OSMPtr(new SortModule(source, sort_by));
}

