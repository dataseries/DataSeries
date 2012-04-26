#ifndef DATASERIES_RENAMECOPIER_HPP
#define DATASERIES_RENAMECOPIER_HPP

#include <boost/foreach.hpp>

#include <DataSeries/GeneralField.hpp>

class RenameCopier {
public:
    typedef boost::shared_ptr<RenameCopier> Ptr;

    RenameCopier(ExtentSeries &source, ExtentSeries &dest)
        : source(source), dest(dest) { }

    void prep(const std::map<std::string, std::string> &copy_columns) {
        typedef std::map<std::string, std::string>::value_type vt;
        BOOST_FOREACH(const vt &copy_column, copy_columns) {
            source_fields.push_back(GeneralField::make(source, copy_column.first));
            dest_fields.push_back(GeneralField::make(dest, copy_column.second));
        }
    }

    void copyRecord() {
        SINVARIANT(!source_fields.empty() && source_fields.size() == dest_fields.size());
        for (size_t i = 0; i < source_fields.size(); ++i) {
            dest_fields[i]->set(source_fields[i]);
        }
    }

    ExtentSeries &source, &dest;
    std::vector<GeneralField::Ptr> source_fields;
    std::vector<GeneralField::Ptr> dest_fields;
};

namespace dataseries {
    std::string renameField(const ExtentType::Ptr type, const std::string &old_name,
                            const std::string &new_name);
}

#endif
