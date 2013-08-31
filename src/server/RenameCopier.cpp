#include "DSSModule.hpp"

string dataseries::renameField(const ExtentType::Ptr type, const string &old_name,
                               const string &new_name) {
    string ret(str(format("  <field name=\"%s\"") % new_name));
    xmlNodePtr type_node = type->xmlNodeFieldDesc(old_name);
        
    for (xmlAttrPtr attr = type_node->properties; NULL != attr; attr = attr->next) {
        if (xmlStrcmp(attr->name, (const xmlChar *)"name") != 0) {
            // TODO: This can't be the right approach, but xmlGetProp() has an internal
            // function ~20 lines long to convert a property into a string. Maybe use
            // xmlAttrSerializeTxtContent somehow?
            xmlChar *tmp = xmlGetProp(type_node, attr->name);
            SINVARIANT(tmp != NULL);
            ret.append(str(format(" %s=\"%s\"") % attr->name % tmp));
            xmlFree(tmp);
        }
    }
    ret.append("/>\n");
    return ret;
}

