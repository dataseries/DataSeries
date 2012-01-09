#ifndef DATASERIES_JOINMODULE_HPP
#define DATASERIES_JOINMODULE_HPP

#include "GVVec.hpp"
#include "ThrowError.hpp"
#include "OutputSeriesModule.hpp"

class JoinModule : public OutputSeriesModule, public ThrowError {
public:
    class Extractor {
    public:
        typedef boost::shared_ptr<Extractor> Ptr;

        Extractor(const std::string &into_field_name) : into_field_name(into_field_name), into() { }
        virtual ~Extractor() { }

        virtual void extract(const GVVec &a_val) = 0;

        const std::string into_field_name; 
        GeneralField::Ptr into;

        static void makeInto(std::vector<Ptr> &extractors, ExtentSeries &series) {
            BOOST_FOREACH(Ptr e, extractors) {
                SINVARIANT(e->into == NULL);
                e->into = GeneralField::make(series, e->into_field_name);
            }
        }
        
        static void extractAll(std::vector<Ptr> &extractors, const GVVec &lookup_val) {
            BOOST_FOREACH(Ptr e, extractors) {
                e->extract(lookup_val);
            }
        }
    };

    // Extract from a field and stuff it into a destination field
    class ExtractorField : public Extractor {
    public:
        virtual ~ExtractorField() { }
        static Ptr make(ExtentSeries &from_series, const std::string &from_field_name,
                        const std::string &into_field_name) {
            GeneralField::Ptr from(GeneralField::make(from_series, from_field_name));
            return Ptr(new ExtractorField(into_field_name, from));
        }

        // TODO: deprecate this version?
        static Ptr make(const std::string &field_name, GeneralField::Ptr from) {
            return Ptr(new ExtractorField(field_name, from));
        }
        virtual void extract(const GVVec &a_val) {
            into->set(from);
        }

    private:
        GeneralField::Ptr from;

        ExtractorField(const std::string &into_field_name, GeneralField::Ptr from) 
            : Extractor(into_field_name), from(from) 
        { }
    };

    // Extract from a value vector and stuff it into a destination field
    class ExtractorValue : public Extractor {
    public:
        virtual ~ExtractorValue() { }
        static Ptr make(const std::string &into_field_name, uint32_t pos) {
            return Ptr(new ExtractorValue(into_field_name, pos));
        }
        
        virtual void extract(const GVVec &a_val) {
            SINVARIANT(pos < a_val.size());
            into->set(a_val.vec[pos]);
        }

    private:
        uint32_t pos;

        ExtractorValue(const std::string &field_name, uint32_t pos) 
            : Extractor(field_name), pos(pos)
        { }
    };
};
        
#endif
