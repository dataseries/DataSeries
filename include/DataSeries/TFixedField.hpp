// -*-C++-*-
/*
   (c) Copyright 2003-2008, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/
/** @file
    Templated simple fixed-size fields (uint8_t, int32_t, int64_t, double)
*/

#ifndef DATASERIES_TFIXEDFIELD_HPP
#define DATASERIES_TFIXEDFIELD_HPP

#include <DataSeries/ExtentType.hpp>
#include <DataSeries/FixedField.hpp>

/// *** EXPERIMENTAL templating to try to reduce overhead of non-nullable
/// fields.
namespace dataseries {
    // In seven years of working with DataSeries, we've only ended up
    // with one option of serious value, nullable.  Therefore, while
    // in theory doing the interface like this could be painful
    // (because we would end up with too many flags), in practice it
    // is likely to work out fine.  One possible improvement would be
    // to make this option a class with two possible sub-values
    // dataseries::NotNullable (default) and dataseries::Nullable
    // (default); making it a bit more self-documenting.  We don't
    // have to decide on this choice until we stop having this feature
    // be experimental.
    template<typename T, bool opt_nullable = false>
    class TFixedField : public FixedField { };

    namespace detail {
	template<typename T>
	struct CppTypeToFieldType { 
	    //	    static const ExtentType::fieldType ft() 
	};

	template<>
	struct CppTypeToFieldType<uint8_t> {
	    static const ExtentType::fieldType ft = ExtentType::ft_byte;
	};
	template<>
	struct CppTypeToFieldType<int32_t> {
	    static const ExtentType::fieldType ft = ExtentType::ft_int32;
	};
	template<>
	struct CppTypeToFieldType<int64_t> {
	    static const ExtentType::fieldType ft = ExtentType::ft_int64;
	};
	template<>
	struct CppTypeToFieldType<double> {
	    static const ExtentType::fieldType ft = ExtentType::ft_double;
	};
	
    };

    template<typename FIELD_TYPE>
    class TFixedField<FIELD_TYPE, false> : public FixedField {
	typedef detail::CppTypeToFieldType<FIELD_TYPE> field_type;
    public:
	TFixedField(ExtentSeries &series, const std::string &field, 
		    bool auto_add = true)
	    : FixedField(series, field, field_type::ft, 0) 
	{
	    // most-derived class needs to run this so the field is
	    // activated.
	    if (auto_add) {
		series.addField(*this);
	    }
	}
	virtual ~TFixedField() { };

	FIELD_TYPE val() const {
	    return *reinterpret_cast<FIELD_TYPE *>(rawval());
	}

	void set(FIELD_TYPE val) {
	    *reinterpret_cast<FIELD_TYPE *>(rawval()) = val;
	    setNull(false);
	}
    };

    // specializations that are nullable
    template<>
    class TFixedField<uint8_t, true> : public ByteField { 
    public:
	TFixedField(ExtentSeries &series, const std::string &field,
		    uint8_t default_value = 0, bool auto_add = true) 
	    : ByteField(series, field, Field::flag_nullable, default_value, 
			false) {
	    if (auto_add) {
		series.addField(*this);
	    }
	}
	virtual ~TFixedField() { };
    };

    template<>
    class TFixedField<int32_t, true> : public Int32Field { 
    public:
	TFixedField(ExtentSeries &series, const std::string &field,
		    int32_t default_value = 0, bool auto_add = true) 
	    : Int32Field(series, field, Field::flag_nullable, default_value,
			 false) {
	    if (auto_add) {
		series.addField(*this);
	    }
	}
	virtual ~TFixedField() { };
    };

    template<>
    class TFixedField<int64_t, true> : public Int64Field {
    public:
	TFixedField(ExtentSeries &series, const std::string &field,
		    int64_t default_value = 0, bool auto_add = true) 
	    : Int64Field(series, field, Field::flag_nullable, default_value,
			 false) {
	    if (auto_add) {
		series.addField(*this);
	    }
	}
	virtual ~TFixedField() { };
    };

    // we sacrifice flag_allownonzerobase, but as commented in
    // DoubleField, this is not a loss.
    template<>
    class TFixedField<double, true> : public DoubleField {
    public:
	TFixedField(ExtentSeries &series, const std::string &field,
		    double default_value = 0.0, bool auto_add = true) 
	    : DoubleField(series, field, Field::flag_nullable, default_value,
			  false) {
	    if (auto_add) {
		series.addField(*this);
	    }
	}
	virtual ~TFixedField() { };
    };
    
};
		    
#endif
