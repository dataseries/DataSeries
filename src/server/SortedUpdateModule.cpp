#include "DSSModule.hpp"

class PrimaryKey {
public:
    PrimaryKey() { }

    void init(ExtentSeries &series, const vector<string> &field_names) {
        fields.reserve(field_names.size());
        BOOST_FOREACH(const string &name, field_names) {
            fields.push_back(GeneralField::make(series, name));
        }
    }

    /// Standard vector ordering by element
    bool operator <(const PrimaryKey &rhs) const {
        SINVARIANT(fields.size() == rhs.fields.size());
        for (size_t i=0; i < fields.size(); ++i) {
            if (*fields[i] < *rhs.fields[i]) {
                return true;
            } else if (*fields[i] > *rhs.fields[i]) {
                return false; // TODO: make sure we have a test for this case; it was missing without a "TEST" failing.
            } // else ==, keep looking
        }
        return false; // equal, so not less than
    }

    bool operator ==(const PrimaryKey &rhs) const {
        SINVARIANT(fields.size() == rhs.fields.size());
        for (size_t i=0; i < fields.size(); ++i) {
            if (*fields[i] != *rhs.fields[i]) {
                return false;
            }
        }
        return true;
    }

    void print(ostream &to) const {
        to << "(";
        BOOST_FOREACH(GeneralField::Ptr f, fields) {
            to << *f;
        }
        to << ")";
    }

    vector<GeneralField::Ptr> fields;
};

ostream &operator <<(ostream &to, const PrimaryKey &key) {
    key.print(to);
    return to;
}

class SortedUpdateModule : public OutputSeriesModule, public ThrowError {
public:
    SortedUpdateModule(DataSeriesModule &base_input, DataSeriesModule &update_input,
                       const string &update_column, const vector<string> &primary_key) 
        : base_input(base_input), update_input(update_input), 
          base_series(), update_series(), base_copier(base_series, output_series),
          update_copier(update_series, output_series), update_column(update_series, update_column),
          primary_key_names(primary_key),  base_primary_key(), update_primary_key()
    { }

    inline bool outputExtentSmall() {
        return output_series.getExtentRef().size() < 96 * 1024;
    }

    virtual Extent::Ptr getSharedExtent() {
        processMergeExtents();
        if (output_series.hasExtent() && outputExtentSmall()) {
            // something more to do..
            SINVARIANT(!base_series.hasExtent() || !update_series.hasExtent());
            if (base_series.hasExtent()) {
                processBaseExtents();
            } else if (update_series.hasExtent()) {
                processUpdateExtents();
            } else {
                // both are done.
            }
        }

        return returnOutputSeries();
    }

    // TODO: lots of cut and paste, near identical code here; seems like we should be able
    // to be more efficient somehow.
    void processMergeExtents() {
        while (true) {
            if (!base_series.hasExtent()) {
                base_series.setExtent(base_input.getSharedExtent());
            }
            if (!update_series.hasExtent()) {
                update_series.setExtent(update_input.getSharedExtent());
            }

            if (!base_series.hasExtent() || !update_series.hasExtent()) {
                LintelLogDebug("SortedUpdate", "no more extents of one type");
                break;
            }

            if (!output_series.hasExtent()) {
                LintelLogDebug("SortedUpdate", "make output extent");
                output_series.setType(base_series.getExtentRef().getTypePtr());
                output_series.newExtent();
            }

            if (!base_series.more()) { // tolerate empty base_series extents
                base_series.clearExtent();
                continue;
            }

            if (!update_series.more()) { // tolerate empty update_series extents
                update_series.clearExtent();
                continue;
            }
            if (!outputExtentSmall()) {
                break;
            }

            if (base_primary_key.fields.empty()) {
                TINVARIANT(update_primary_key.fields.empty());
                base_primary_key.init(base_series, primary_key_names);
                update_primary_key.init(update_series, primary_key_names);
                base_copier.prep();
                update_copier.prep();
            }
           
            if (base_primary_key < update_primary_key) {
                copyBase();
                advance(base_series);
            } else {
                doUpdate();
                advance(update_series);
            }
        }
    }

    void processBaseExtents() {
        SINVARIANT(update_input.getSharedExtent() == NULL);
        LintelLogDebug("SortedUpdate", "process base only...");
        while (true) {
            if (!base_series.hasExtent()) {
                base_series.setExtent(base_input.getSharedExtent());
            }

            if (!base_series.hasExtent()) {
                LintelLogDebug("SortedUpdate", "no more base extents");
                break;
            }

            if (!output_series.hasExtent()) {
                LintelLogDebug("SortedUpdate", "make output extent");
                output_series.setType(base_series.getExtentRef().getTypePtr());
                output_series.newExtent();
            }

            if (!outputExtentSmall()) {
                break;
            }

            copyBase();
            advance(base_series);
        }            
    }

    void processUpdateExtents() {
        SINVARIANT(base_input.getSharedExtent() == NULL);
        LintelLogDebug("SortedUpdate", "process update only...");
        while (true) {
            if (!update_series.hasExtent()) {
                update_series.setExtent(update_input.getSharedExtent());
            }

            if (!update_series.hasExtent()) {
                LintelLogDebug("SortedUpdate", "no more update extents");
                break;
            }

            if (!output_series.hasExtent()) {
                LintelLogDebug("SortedUpdate", "make output extent");
                output_series.setType(update_series.getSharedExtent()->getTypePtr());
                output_series.newExtent();
            }

            if (!outputExtentSmall()) {
                break;
            }

            doUpdate();
            advance(update_series);
        }            
    }

    void copyBase() {
        LintelLogDebug("SortedUpdate", "copy-base");
        output_series.newRecord();
        base_copier.copyRecord();
    }

    void doUpdate() {
        switch (update_column()) 
            {
            case 1: doUpdateInsert(); break;
            case 2: doUpdateReplace(); break;
            case 3: doUpdateDelete(); break;
            default: requestError(format("invalid update column value %d")
                                  % static_cast<uint32_t>(update_column()));
            }
    }

    void doUpdateInsert() {
        copyUpdate();
    }

    void doUpdateReplace() {
        if (base_series.hasExtent() && base_primary_key == update_primary_key) {
            copyUpdate();
            advance(base_series);
        } else {
            copyUpdate(); // equivalent to insert
        }
    }

    void doUpdateDelete() {
        if (base_series.hasExtent() && base_primary_key == update_primary_key) {
            advance(base_series);
        } else {
            // already deleted
        }
    }

    void copyUpdate() {
        LintelLogDebug("SortedUpdate", "copy-update");
        output_series.newRecord();
        update_copier.copyRecord();
    }

    void advance(ExtentSeries &series) {
        series.next();
        if (!series.more()) {
            series.clearExtent();
        }
    }

    DataSeriesModule &base_input, &update_input;
    ExtentSeries base_series, update_series;
    ExtentRecordCopy base_copier, update_copier;
    TFixedField<uint8_t> update_column;
    const vector<string> primary_key_names;
    PrimaryKey base_primary_key, update_primary_key;
};

DataSeriesModule::Ptr 
dataseries::makeSortedUpdateModule(DataSeriesModule &base_input, DataSeriesModule &update_input,
                                   const string &update_column, const vector<string> &primary_key) {
    return DataSeriesModule::Ptr(new SortedUpdateModule(base_input, update_input, update_column, 
                                                        primary_key));
}

